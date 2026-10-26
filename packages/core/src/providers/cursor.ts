// Copyright (C) 2026 tommy0103 and contributors.
// SPDX-License-Identifier: AGPL-3.0-only

// Cursor provider adapter. Reads the global composer store at state.vscdb.
// Pure: it never writes the source database. SQLite opening is injected because
// Core runs under both node:sqlite (CLI) and better-sqlite3 (Electron).

import { createHash } from 'node:crypto';
import { readFileSync, statSync } from 'node:fs';
import { homedir } from 'node:os';
import { basename, dirname, isAbsolute, join, normalize, sep } from 'node:path';

import { normalizeObservedCwd, projectSlugFromPath } from '../parsing.ts';
import type { SqliteDb } from '../sqlite-types.ts';
import { RetryableSourceReadError } from './source-read-error.ts';
import type {
  Cursor,
  DiscoverContext,
  IndexUnit,
  MessageRecord,
  ProviderAdapter,
  RawLookup,
  RawRecord,
  SubagentRecord,
  ToolCallRecord,
  ToolResultRecord,
  TranscriptRecord,
  WatchTarget,
} from './types.ts';

export const name = 'cursor';
export const CURSOR_CANONICAL_TRANSCRIPT_MARKER = '__cursor_canonical_transcript_v1__';

const TEXT_LIMIT = 10000;
const TOMBSTONE_CURSOR = 'cursor-tombstone-v1';

export type CursorDatabaseOpener = (dbPath: string) => SqliteDb;

type JsonRecord = Record<string, unknown>;

interface StoreStamp {
  readonly identity: string;
  readonly stamp: string;
}

interface ComposerCensus {
  readonly composerId: string;
  readonly raw: string;
  readonly value: JsonRecord;
  readonly fingerprint: string;
  readonly isSubagent: boolean;
  readonly incomplete: boolean;
}

interface CursorUnitMeta {
  readonly kind: 'cursor-session' | 'cursor-stamp' | 'cursor-tombstone' | 'cursor-observation';
  readonly dbPath: string;
  readonly stamp: string;
  readonly identity: string;
  readonly composerId?: string;
  readonly fingerprint?: string;
  readonly isSubagent?: boolean;
}

export function defaultCursorDatabasePath(
  homeDir = homedir(),
  platform = process.platform,
): string {
  if (platform === 'darwin') {
    return join(homeDir, 'Library', 'Application Support', 'Cursor', 'User', 'globalStorage', 'state.vscdb');
  }
  if (platform === 'win32') {
    const appData = process.env.APPDATA;
    const base = appData !== undefined && appData.trim().length > 0
      ? appData.trim()
      : join(homeDir, 'AppData', 'Roaming');
    return join(base, 'Cursor', 'User', 'globalStorage', 'state.vscdb');
  }
  return join(homeDir, '.config', 'Cursor', 'User', 'globalStorage', 'state.vscdb');
}

export function resolveCursorDatabase(
  rootDir: string | undefined,
  homeDir = homedir(),
  platform = process.platform,
): string {
  if (rootDir !== undefined && rootDir.trim().length > 0) {
    const trimmed = rootDir.trim();
    const expanded = trimmed === '~'
      ? homeDir
      : trimmed.startsWith('~/') || trimmed.startsWith('~\\')
        ? join(homeDir, trimmed.slice(2))
        : trimmed;
    const resolved = normalize(expanded);
    if (resolved.endsWith('.vscdb')) return resolved;
    if (basename(resolved) === 'globalStorage') return join(resolved, 'state.vscdb');
    return join(resolved, 'User', 'globalStorage', 'state.vscdb');
  }
  return defaultCursorDatabasePath(homeDir, platform);
}

export function cursorStoreHash(dbPath: string): string {
  return createHash('sha256').update(`cursor-store-v1\0${normalize(dbPath)}`).digest('hex');
}

export function cursorSessionId(composerId: string, dbPath: string): string {
  return `cursor:${encodeURIComponent(composerId)}:${cursorStoreHash(dbPath)}`;
}

export function cursorUnitKey(dbPath: string, composerId: string): string {
  return `${normalize(dbPath)}#cmp:${encodeURIComponent(composerId)}`;
}

export function decodeCursorUnitKey(key: string): { dbPath: string; composerId: string } | null {
  const delimiter = key.lastIndexOf('#cmp:');
  if (delimiter < 1) return null;
  try {
    const dbPath = normalize(key.slice(0, delimiter));
    const composerId = decodeURIComponent(key.slice(delimiter + 5));
    if (!composerId || cursorUnitKey(dbPath, composerId) !== key) return null;
    return { dbPath, composerId };
  } catch {
    return null;
  }
}

function observationKey(dbPath: string): string {
  return `__cursor_observation_v1__:${cursorStoreHash(dbPath)}`;
}

function observationCursor(stamp: string): string {
  return `cursor-obs-v1\0${stamp}`;
}

function sessionCursor(stamp: string, fingerprint: string): string {
  return `cursor-session-v1\0${stamp}\0${fingerprint}`;
}

function sessionFingerprint(cursor: string | null): string | null {
  if (cursor === null || !cursor.startsWith('cursor-session-v1\0')) return null;
  const fingerprint = cursor.split('\0')[2];
  return fingerprint !== undefined && fingerprint.length > 0 ? fingerprint : null;
}

function storePhysicalState(dbPath: string): StoreStamp {
  const main = statSync(dbPath, { bigint: true });
  let wal;
  try { wal = statSync(`${dbPath}-wal`, { bigint: true }); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }
  const fields = (stat: typeof main | undefined) => stat === undefined ? null : [
    stat.dev, stat.ino, stat.birthtimeNs, stat.mtimeNs, stat.ctimeNs, stat.size,
  ].map(String);
  const digest = (value: unknown) => createHash('sha256').update(JSON.stringify(value)).digest('hex');
  return {
    identity: digest([String(main.dev), String(main.ino), String(main.birthtimeNs)]),
    stamp: digest([fields(main), fields(wal)]),
  };
}

function openSource(dbPath: string, opener: CursorDatabaseOpener): SqliteDb | null {
  try {
    return opener(dbPath);
  } catch {
    return null;
  }
}

function asText(value: unknown): string | null {
  if (typeof value === 'string') return value;
  if (value instanceof Uint8Array) return Buffer.from(value).toString('utf8');
  return null;
}

function sliceText(value: string, limit: number): string {
  if (value.length <= limit) return value;
  const boundary = value.charCodeAt(limit - 1);
  const splitsPair = boundary >= 0xd800 && boundary <= 0xdbff;
  return value.slice(0, splitsPair ? limit - 1 : limit);
}

function trunc(value: string): string {
  return sliceText(value, TEXT_LIMIT);
}

function isRecord(value: unknown): value is JsonRecord {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function filesystemPath(value: unknown): string | null {
  if (typeof value === 'string') {
    if (value.startsWith('file://')) {
      try {
        const pathname = decodeURIComponent(new URL(value).pathname);
        if (process.platform === 'win32' && /^\/[A-Za-z]:/.test(pathname)) return pathname.slice(1);
        return pathname;
      } catch {
        return null;
      }
    }
    return normalizeObservedCwd(value);
  }
  if (!isRecord(value)) return null;
  return filesystemPath(value.fsPath)
    ?? filesystemPath(value.path)
    ?? filesystemPath(value.uri)
    ?? filesystemPath(value.external);
}

function stringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === 'string' && item.length > 0);
}

function composerIdFromKey(key: string): string | null {
  const prefix = 'composerData:';
  if (!key.startsWith(prefix)) return null;
  const composerId = key.slice(prefix.length);
  return composerId.length > 0 ? composerId : null;
}

function isoTimestamp(value: unknown): string | null {
  if (typeof value === 'string' && value.trim().length > 0) {
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? value : new Date(parsed).toISOString();
  }
  if (typeof value === 'number' && Number.isFinite(value)) return new Date(value).toISOString();
  return null;
}

function numberOrNull(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function readRow(db: SqliteDb, key: string): string | null {
  const row = db.prepare('SELECT value FROM cursorDiskKV WHERE key = ?').get(key);
  return row === undefined ? null : asText(row.value);
}

function hasComposerTable(db: SqliteDb): boolean {
  const row = db.prepare(
    "SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = 'cursorDiskKV'",
  ).get();
  return row !== undefined;
}

function projectFromWorkspace(dbPath: string, workspaceId: string | null): string | null {
  if (workspaceId === null || workspaceId.length === 0) return null;
  const workspaceJson = join(dirname(dirname(dbPath)), 'workspaceStorage', workspaceId, 'workspace.json');
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(workspaceJson, 'utf8'));
  } catch {
    return null;
  }
  if (!isRecord(parsed)) return null;
  return filesystemPath(parsed.folder) ?? filesystemPath(parsed.workspace);
}

function bubbleFilePath(params: unknown): string | null {
  if (!isRecord(params)) return null;
  return filesystemPath(params.path)
    ?? filesystemPath(params.file_path)
    ?? filesystemPath(params.filePath)
    ?? filesystemPath(params.target_file)
    ?? filesystemPath(params.target_notebook);
}

function toolPayloads(value: unknown): JsonRecord[] {
  if (Array.isArray(value)) return value.filter(isRecord);
  return isRecord(value) ? [value] : [];
}

function toolResultText(result: unknown): string {
  if (typeof result === 'string') return result;
  if (isRecord(result)) {
    for (const key of ['content', 'output', 'text', 'error']) {
      if (typeof result[key] === 'string') return result[key];
    }
  }
  if (result === undefined) return '';
  try { return JSON.stringify(result); }
  catch { return ''; }
}

function thinkingText(value: unknown): string | null {
  if (!Array.isArray(value)) return null;
  const parts: string[] = [];
  for (const block of value) {
    if (typeof block === 'string' && block.trim().length > 0) parts.push(block);
    else if (isRecord(block)) {
      const text = block.text ?? block.content;
      if (typeof text === 'string' && text.trim().length > 0) parts.push(text);
    }
  }
  return parts.length > 0 ? parts.join('\n') : null;
}

function workspaceIdOf(value: JsonRecord): string | null {
  const identifier = value.workspaceIdentifier;
  if (!isRecord(identifier) || typeof identifier.id !== 'string' || identifier.id.length === 0) return null;
  return identifier.id;
}

function childIds(value: JsonRecord): { subagent: string[]; subcomposer: string[] } {
  return {
    subagent: stringList(value.subagentComposerIds),
    subcomposer: stringList(value.subComposerIds),
  };
}

function messageUuid(sessionId: string, bubbleId: string): string {
  return `${sessionId}:b:${encodeURIComponent(bubbleId)}`;
}

function projectBubble(
  sessionId: string,
  bubbleId: string,
  bubble: JsonRecord,
  parent: string | null,
  cwd: string | null,
  isSubagent: boolean,
): TranscriptRecord[] {
  const records: TranscriptRecord[] = [];
  const type = bubble.type;
  const role = type === 1 ? 'user' : type === 2 ? 'assistant' : null;
  const visible = typeof bubble.text === 'string' && bubble.text.length > 0 ? bubble.text : null;
  const thinking = visible === null ? thinkingText(bubble.allThinkingBlocks) : null;
  const text = visible ?? thinking;
  const tools = toolPayloads(bubble.toolFormerData);
  const contentType = text === null
    ? (tools.length > 0 ? 'tool_use' : 'unknown')
    : (thinking !== null && visible === null ? 'thinking' : 'text');
  const tokens = isRecord(bubble.tokenCount) ? bubble.tokenCount : {};
  const model = isRecord(bubble.modelInfo) && typeof bubble.modelInfo.modelName === 'string'
    ? bubble.modelInfo.modelName
    : null;
  const uuid = messageUuid(sessionId, bubbleId);
  const message: MessageRecord = {
    kind: 'message',
    uuid,
    session_id: sessionId,
    type: role ?? 'cursor',
    parent_uuid: parent,
    timestamp: isoTimestamp(bubble.createdAt),
    role,
    text: text === null ? null : trunc(text),
    content_type: contentType,
    is_meta: 0,
    visibility: 'visible',
    model,
    is_sidechain: isSubagent ? 1 : 0,
    agent_id: isSubagent ? sessionId : null,
    input_tokens: numberOrNull(tokens.inputTokens),
    output_tokens: numberOrNull(tokens.outputTokens),
    cwd,
    skill: null,
    source: name,
  };
  records.push(message);
  tools.forEach((tool, index) => {
    const nativeId = typeof tool.toolCallId === 'string' && tool.toolCallId.length > 0
      ? tool.toolCallId
      : `${bubbleId}:${index}`;
    const toolId = `${sessionId}:tool:${encodeURIComponent(nativeId)}`;
    const toolName = typeof tool.name === 'string' && tool.name.length > 0 ? tool.name : 'tool';
    const params = tool.params !== undefined ? tool.params : tool.rawArgs;
    let inputJson: string;
    try { inputJson = trunc(typeof params === 'string' ? params : JSON.stringify(params ?? {})); }
    catch { inputJson = '{}'; }
    const call: ToolCallRecord = {
      kind: 'tool_call',
      id: toolId,
      message_uuid: uuid,
      session_id: sessionId,
      name: toolName,
      presentation: toolName === 'Skill' ? 'skill' : 'default',
      input_json: inputJson,
      file_path: bubbleFilePath(tool.params),
    };
    const status = typeof tool.status === 'string' ? tool.status.toLowerCase() : '';
    const result: ToolResultRecord = {
      kind: 'tool_result',
      tool_use_id: toolId,
      message_uuid: uuid,
      session_id: sessionId,
      content: trunc(toolResultText(tool.result)),
      file_path: call.file_path,
      is_error: status === 'error' || status === 'failed' || status === 'cancelled' ? 1 : 0,
    };
    records.push(call, result);
  });
  return records;
}

function headerIds(value: JsonRecord): string[] {
  if (!Array.isArray(value.fullConversationHeadersOnly)) return [];
  const ids: string[] = [];
  for (const header of value.fullConversationHeadersOnly) {
    if (isRecord(header) && typeof header.bubbleId === 'string' && header.bubbleId.length > 0) {
      ids.push(header.bubbleId);
    }
  }
  return ids;
}

function loadComposer(
  db: SqliteDb,
  dbPath: string,
  composerId: string,
  subagentIds: ReadonlySet<string>,
): ComposerCensus | null {
  const raw = readRow(db, `composerData:${composerId}`);
  if (raw === null) return null;
  let value: unknown;
  try { value = JSON.parse(raw); }
  catch { return { composerId, raw, value: {}, fingerprint: '', isSubagent: subagentIds.has(composerId), incomplete: true }; }
  if (!isRecord(value)) {
    return { composerId, raw, value: {}, fingerprint: '', isSubagent: subagentIds.has(composerId), incomplete: true };
  }
  const hash = createHash('sha256');
  hash.update(subagentIds.has(composerId) ? 'subagent\0' : 'root\0');
  hash.update(raw);
  let incomplete = false;
  for (const bubbleId of headerIds(value)) {
    const bubble = readRow(db, `bubbleId:${composerId}:${bubbleId}`);
    if (bubble === null) {
      incomplete = true;
      break;
    }
    hash.update('\0');
    hash.update(bubble);
  }
  return {
    composerId,
    raw,
    value,
    fingerprint: hash.digest('hex'),
    isSubagent: subagentIds.has(composerId),
    incomplete,
  };
}

function projectPathFor(dbPath: string, composer: JsonRecord, bubbles: JsonRecord[]): string | null {
  for (const bubble of bubbles) {
    if (!Array.isArray(bubble.workspaceUris)) continue;
    for (const uri of bubble.workspaceUris) {
      const path = filesystemPath(uri);
      if (path !== null) return path;
    }
  }
  return projectFromWorkspace(dbPath, workspaceIdOf(composer));
}

function projectComposer(
  db: SqliteDb,
  dbPath: string,
  census: ComposerCensus,
): TranscriptRecord[] {
  const sessionId = cursorSessionId(census.composerId, dbPath);
  const bubbles: Array<{ id: string; value: JsonRecord }> = [];
  for (const bubbleId of headerIds(census.value)) {
    const raw = readRow(db, `bubbleId:${census.composerId}:${bubbleId}`);
    if (raw === null) throw new RetryableSourceReadError(dbPath, `cursor: bubble disappeared during parse: ${bubbleId}`);
    let parsed: unknown;
    try { parsed = JSON.parse(raw); }
    catch { throw new RetryableSourceReadError(dbPath, `cursor: bubble JSON is unreadable: ${bubbleId}`); }
    if (!isRecord(parsed)) throw new RetryableSourceReadError(dbPath, `cursor: bubble JSON is unreadable: ${bubbleId}`);
    bubbles.push({ id: bubbleId, value: parsed });
  }
  const cwd = projectPathFor(dbPath, census.value, bubbles.map(bubble => bubble.value));
  const records: TranscriptRecord[] = [];
  let parent: string | null = null;
  let count = 0;
  for (const bubble of bubbles) {
    const projected = projectBubble(sessionId, bubble.id, bubble.value, parent, cwd, census.isSubagent);
    const message = projected.find((record): record is MessageRecord => record.kind === 'message');
    if (message !== undefined) {
      parent = message.uuid;
      count += 1;
    }
    records.push(...projected);
  }
  const children = childIds(census.value);
  const linked = new Set<string>();
  for (const child of children.subagent) linked.add(child);
  for (const child of children.subcomposer) linked.add(child);
  for (const child of linked) {
    const record: SubagentRecord = {
      kind: 'subagent',
      agent_id: cursorSessionId(child, dbPath),
      session_id: sessionId,
      parent_tool_use_id: null,
      agent_type: children.subagent.includes(child) ? 'subagent' : 'subcomposer',
      description: null,
    };
    records.push(record);
  }
  const titleSource = typeof census.value.name === 'string' && census.value.name.trim().length > 0
    ? census.value.name
    : typeof census.value.subtitle === 'string' && census.value.subtitle.trim().length > 0
      ? census.value.subtitle
      : records.find((record): record is MessageRecord => record.kind === 'message' && record.role === 'user' && record.text !== null)?.text ?? null;
  const started = isoTimestamp(census.value.createdAt) ?? records.find((record): record is MessageRecord => record.kind === 'message')?.timestamp ?? null;
  const ended = [...records].reverse().find((record): record is MessageRecord => record.kind === 'message' && record.timestamp !== null)?.timestamp ?? started;
  records.push({
    kind: 'session',
    id: sessionId,
    title: titleSource === null ? null : sliceText(titleSource, 120),
    project: projectSlugFromPath(cwd),
    started_at: started,
    ended_at: ended,
    git_branch: null,
    version: census.value._v === undefined || census.value._v === null ? null : String(census.value._v),
    message_count: count,
    countMode: 'total',
    jsonl_path: cursorUnitKey(dbPath, census.composerId),
    source: name,
  });
  return records;
}

function listComposerIds(db: SqliteDb): Map<string, string> {
  const rows = db.prepare(
    "SELECT key, value FROM cursorDiskKV WHERE key GLOB 'composerData:*'",
  ).all();
  const composers = new Map<string, string>();
  for (const row of rows) {
    const key = typeof row.key === 'string' ? row.key : null;
    const raw = asText(row.value);
    if (key === null || raw === null) continue;
    const composerId = composerIdFromKey(key);
    if (composerId !== null) composers.set(composerId, raw);
  }
  return composers;
}

function subagentIdsFrom(composers: ReadonlyMap<string, string>): Set<string> {
  const ids = new Set<string>();
  for (const raw of composers.values()) {
    try {
      const parsed = JSON.parse(raw) as unknown;
      if (!isRecord(parsed)) continue;
      for (const id of [...childIds(parsed).subagent, ...childIds(parsed).subcomposer]) ids.add(id);
    } catch {
      // Malformed composer JSON is reported when that composer is loaded.
    }
  }
  return ids;
}

function discoverDatabase(
  dbPath: string,
  ctx: DiscoverContext,
  opener: CursorDatabaseOpener,
): IndexUnit[] {
  const walPath = `${dbPath}-wal`;
  if (ctx.changedPaths !== undefined) {
    const normalized = normalize(dbPath);
    const touched = ctx.changedPaths.some((changed) => {
      const absolute = isAbsolute(changed) ? normalize(changed) : normalized;
      return absolute === normalized || absolute === normalize(walPath);
    });
    if (!touched) return [];
  }

  let present: 'yes' | 'missing' | 'inaccessible';
  try {
    statSync(dbPath);
    present = 'yes';
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    present = code === 'ENOENT' || code === 'ENOTDIR' ? 'missing' : 'inaccessible';
  }
  const prior = (ctx.indexedSessions?.() ?? []).filter((session) => {
    const identity = decodeCursorUnitKey(session.jsonlPath);
    return identity !== null && identity.dbPath === normalize(dbPath);
  });
  if (present === 'inaccessible' || present === 'missing') {
    if (present === 'inaccessible' || prior.length > 0) {
      ctx.reportIncompleteInventory?.({
        path: dbPath,
        error: present === 'missing' ? 'Source database is unavailable' : 'Source database is not accessible',
      });
    }
    return [];
  }

  let physical: StoreStamp;
  try {
    physical = storePhysicalState(dbPath);
  } catch (error) {
    ctx.reportIncompleteInventory?.({
      path: dbPath,
      error: error instanceof Error ? error.message : String(error),
    });
    return [];
  }
  const currentObservation = observationCursor(physical.stamp);
  const indexedCurrent = prior.length === 0 || prior.every((session) => {
    const cursor = ctx.lastCursor(session.jsonlPath);
    return cursor !== null && cursor.startsWith(`cursor-session-v1\0${physical.stamp}\0`);
  });
  if (ctx.lastCursor(observationKey(dbPath)) === currentObservation && indexedCurrent) return [];

  const db = openSource(dbPath, opener);
  if (db === null) {
    ctx.reportIncompleteInventory?.({ path: dbPath, error: 'Session database is busy or unreadable' });
    return [];
  }
  try {
    db.exec('BEGIN');
    try {
      // Stat before the first read. BEGIN is deferred, so this is the stamp the
      // snapshot is racing. A later write does not tear that snapshot; it only
      // means this pass must not claim the store is caught up.
      const snapshot = storePhysicalState(dbPath);
      if (snapshot.identity !== physical.identity) {
        throw new RetryableSourceReadError(dbPath, 'cursor: source file was replaced during inventory');
      }
      physical = snapshot;
      if (!hasComposerTable(db)) {
        ctx.reportIncompleteInventory?.({ path: dbPath, error: 'Cursor database has no composer table' });
        return [];
      }
      const composers = listComposerIds(db);
      const children = subagentIdsFrom(composers);
      const units: IndexUnit[] = [];
      const live = new Set<string>();
      for (const composerId of [...composers.keys()].sort()) {
        const census = loadComposer(db, dbPath, composerId, children);
        if (census === null) continue;
        const sessionId = cursorSessionId(composerId, dbPath);
        const key = cursorUnitKey(dbPath, composerId);
        live.add(sessionId);
        if (census.incomplete || census.fingerprint.length === 0) {
          ctx.reportSkippedArtifact?.({ path: key, error: `Unreadable Cursor composer ${composerId}` });
          ctx.retainSession?.(sessionId);
          continue;
        }
        const cursor = sessionCursor(physical.stamp, census.fingerprint);
        const previous = ctx.lastCursor(key);
        if (previous === cursor) continue;
        if (sessionFingerprint(previous) === census.fingerprint) {
          units.push({
            key,
            sessionId: '',
            meta: {
              kind: 'cursor-stamp',
              dbPath: normalize(dbPath),
              stamp: physical.stamp,
              identity: physical.identity,
              composerId,
              fingerprint: census.fingerprint,
              isSubagent: census.isSubagent,
            } satisfies CursorUnitMeta,
          });
          continue;
        }
        units.push({
          key,
          sessionId,
          isSubagent: census.isSubagent,
          agentId: census.isSubagent ? sessionId : undefined,
          retractSessionIds: [sessionId],
          meta: {
            kind: 'cursor-session',
            dbPath: normalize(dbPath),
            stamp: physical.stamp,
            identity: physical.identity,
            composerId,
            fingerprint: census.fingerprint,
            isSubagent: census.isSubagent,
          } satisfies CursorUnitMeta,
        });
      }
      const settled = storePhysicalState(dbPath);
      if (settled.identity !== physical.identity) {
        throw new RetryableSourceReadError(dbPath, 'cursor: source file was replaced during inventory');
      }
      // Tombstones and the observation cursor both claim this stamp is fully
      // observed. A concurrent write keeps the sessions read from the snapshot
      // and leaves the previous index in place for anything this pass did not see.
      const quiet = settled.stamp === physical.stamp;
      if (quiet) {
        for (const indexed of prior) {
          if (live.has(indexed.sessionId)) continue;
          const identity = decodeCursorUnitKey(indexed.jsonlPath);
          if (identity === null || cursorSessionId(identity.composerId, dbPath) !== indexed.sessionId) continue;
          units.push({
            key: indexed.jsonlPath,
            sessionId: indexed.sessionId,
            retractSessionIds: [indexed.sessionId],
            meta: {
              kind: 'cursor-tombstone',
              dbPath: normalize(dbPath),
              stamp: physical.stamp,
              identity: physical.identity,
              composerId: identity.composerId,
            } satisfies CursorUnitMeta,
          });
        }
        if (ctx.lastCursor(observationKey(dbPath)) !== observationCursor(physical.stamp) || units.length > 0) {
          units.push({
            key: observationKey(dbPath),
            sessionId: '',
            meta: {
              kind: 'cursor-observation',
              dbPath: normalize(dbPath),
              stamp: physical.stamp,
              identity: physical.identity,
            } satisfies CursorUnitMeta,
          });
        }
      }
      return units;
    } finally {
      db.exec('COMMIT');
    }
  } catch (error) {
    ctx.reportIncompleteInventory?.({
      path: dbPath,
      error: error instanceof Error ? error.message : String(error),
    });
    return [];
  } finally {
    db.close();
  }
}

function metaOf(unit: IndexUnit): CursorUnitMeta {
  const meta = unit.meta as CursorUnitMeta | undefined;
  if (meta === undefined || meta.kind === undefined) throw new Error('cursor: unit metadata is missing');
  return meta;
}

function* parse(unit: IndexUnit, opener: CursorDatabaseOpener): Generator<TranscriptRecord, Cursor> {
  const meta = metaOf(unit);
  const physical = storePhysicalState(meta.dbPath);
  if (physical.identity !== meta.identity) {
    throw new RetryableSourceReadError(meta.dbPath, 'cursor: source file was replaced before parse');
  }
  if (meta.kind === 'cursor-observation') return observationCursor(meta.stamp);
  if (meta.kind === 'cursor-stamp') {
    const identity = decodeCursorUnitKey(unit.key);
    if (
      meta.fingerprint === undefined
      || meta.composerId === undefined
      || identity === null
      || identity.dbPath !== normalize(meta.dbPath)
      || identity.composerId !== meta.composerId
    ) {
      throw new Error('cursor: stamp unit does not match its composer');
    }
    return sessionCursor(meta.stamp, meta.fingerprint);
  }

  const identity = decodeCursorUnitKey(unit.key);
  if (
    identity === null
    || identity.dbPath !== normalize(meta.dbPath)
    || identity.composerId !== meta.composerId
    || cursorSessionId(identity.composerId, identity.dbPath) !== unit.sessionId
  ) {
    throw new Error('cursor: unit identity does not match metadata');
  }

  const db = openSource(meta.dbPath, opener);
  if (db === null) throw new RetryableSourceReadError(meta.dbPath, 'cursor: session database is busy or unreadable');
  try {
    db.exec('BEGIN');
    try {
      if (!hasComposerTable(db)) throw new RetryableSourceReadError(meta.dbPath, 'cursor: composer table disappeared');
      const exists = readRow(db, `composerData:${meta.composerId}`) !== null;
      if (meta.kind === 'cursor-tombstone') {
        if (exists) throw new RetryableSourceReadError(meta.dbPath, 'cursor: composer still present during tombstone');
        const tombstoneStamp = storePhysicalState(meta.dbPath);
        if (tombstoneStamp.identity !== meta.identity || tombstoneStamp.stamp !== meta.stamp) {
          throw new RetryableSourceReadError(meta.dbPath, 'cursor: source changed during tombstone verification');
        }
        return TOMBSTONE_CURSOR;
      }
      if (!exists || meta.fingerprint === undefined) {
        throw new RetryableSourceReadError(meta.dbPath, 'cursor: composer disappeared before parse');
      }
      const children = subagentIdsFrom(listComposerIds(db));
      const census = loadComposer(db, meta.dbPath, meta.composerId, children);
      if (
        census === null
        || census.incomplete
        || census.fingerprint !== meta.fingerprint
        || census.isSubagent !== meta.isSubagent
      ) {
        throw new RetryableSourceReadError(meta.dbPath, 'cursor: source changed before parse');
      }
      yield* projectComposer(db, meta.dbPath, census);
      return sessionCursor(meta.stamp, census.fingerprint);
    } finally {
      db.exec('COMMIT');
    }
  } finally {
    db.close();
  }
}

function bubbleText(bubble: JsonRecord): string | null {
  if (typeof bubble.text === 'string' && bubble.text.length > 0) return bubble.text;
  return thinkingText(bubble.allThinkingBlocks);
}

function raw(input: RawLookup, opener: CursorDatabaseOpener): RawRecord | null {
  const jsonlPath = input.session?.jsonl_path;
  if (typeof jsonlPath !== 'string') return null;
  const identity = decodeCursorUnitKey(jsonlPath);
  if (identity === null) return null;
  const encoded = input.messageUuid.startsWith(`${cursorSessionId(identity.composerId, identity.dbPath)}:b:`)
    ? input.messageUuid.slice(cursorSessionId(identity.composerId, identity.dbPath).length + 3)
    : null;
  if (encoded === null) return null;
  let bubbleId: string;
  try { bubbleId = decodeURIComponent(encoded); }
  catch { return null; }
  const db = openSource(identity.dbPath, opener);
  if (db === null) return null;
  try {
    db.exec('BEGIN');
    try {
      const rawBubble = readRow(db, `bubbleId:${identity.composerId}:${bubbleId}`);
      if (rawBubble === null) return null;
      const parsed = JSON.parse(rawBubble) as unknown;
      if (!isRecord(parsed)) return null;
      const text = bubbleText(parsed);
      return {
        text: rawBubble,
        totalLength: rawBubble.length,
        hasMore: false,
        messageText: text,
      };
    } finally {
      db.exec('COMMIT');
    }
  } catch {
    return null;
  } finally {
    db.close();
  }
}

export function createCursorProvider({
  rootDir,
  homeDir = homedir(),
  platform = process.platform,
  openDatabase,
}: {
  rootDir?: string;
  homeDir?: string;
  platform?: NodeJS.Platform;
  openDatabase?: CursorDatabaseOpener;
} = {}): ProviderAdapter {
  const opener = openDatabase ?? (() => {
    throw new Error('No CursorDatabaseOpener injected');
  });
  const databasePath = resolveCursorDatabase(rootDir, homeDir, platform);
  return {
    name,
    descriptor: {
      id: name,
      name: 'Cursor',
      vendor: 'Anysphere',
      defaultRoot: databasePath,
      color: '#000000',
    },
    indexVersionMarker: CURSOR_CANONICAL_TRANSCRIPT_MARKER,
    sessionUnitKey: (session) => session.jsonlPath,
    watchHints: (unitKey) => {
      const identity = decodeCursorUnitKey(unitKey);
      if (identity === null || identity.dbPath !== normalize(databasePath)) return [];
      return [identity.dbPath, `${identity.dbPath}-wal`];
    },
    discover: (ctx) => discoverDatabase(databasePath, ctx, opener),
    parse: (unit) => parse(unit, opener),
    watchTargets: (configuredRoot: string): WatchTarget[] => {
      const configured = configuredRoot.trim().length > 0 ? configuredRoot : databasePath;
      const dbPath = resolveCursorDatabase(
        configured === databasePath ? databasePath : configured,
        homeDir,
        platform,
      );
      return [
        { kind: 'file', path: dbPath },
        { kind: 'file', path: `${dbPath}-wal` },
      ];
    },
    raw: (input) => raw(input, opener),
  };
}
