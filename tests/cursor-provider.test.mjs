// Copyright (C) 2026 tommy0103 and contributors.
// SPDX-License-Identifier: AGPL-3.0-only

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

import { persist } from '../packages/core/src/persist.ts';
import { assembleSessionDetail } from '../packages/core/src/session-detail.ts';
import {
  createCursorProvider,
  cursorSessionId,
  cursorUnitKey,
  defaultCursorDatabasePath,
  resolveCursorDatabase,
} from '../packages/core/src/providers/cursor.ts';
import { openCursorDatabaseWithNodeSqlite } from '../packages/core/src/providers/cursor-node.ts';
import { makeTempDir } from './temp-dirs.mjs';

const SCHEMA = readFileSync(new URL('../packages/core/src/schema.sql', import.meta.url), 'utf8');

function initStore(dbPath) {
  mkdirSync(join(dbPath, '..'), { recursive: true });
  const db = new DatabaseSync(dbPath);
  db.exec('CREATE TABLE cursorDiskKV (key TEXT PRIMARY KEY, value TEXT)');
  db.close();
}

function insert(dbPath, key, value) {
  const db = new DatabaseSync(dbPath);
  db.prepare('INSERT OR REPLACE INTO cursorDiskKV (key, value) VALUES (?, ?)').run(key, JSON.stringify(value));
  db.close();
}

function seedConversation(dbPath) {
  initStore(dbPath);
  insert(dbPath, 'composerData:cmp-parent', {
    _v: 12,
    composerId: 'cmp-parent',
    name: 'Auth bug',
    createdAt: 1710000000000,
    workspaceIdentifier: { id: 'ws1234567890' },
    subagentComposerIds: ['cmp-child'],
    fullConversationHeadersOnly: [
      { bubbleId: 'b-user', type: 1 },
      { bubbleId: 'b-empty', type: 2 },
      { bubbleId: 'b-tool', type: 2 },
    ],
  });
  insert(dbPath, 'bubbleId:cmp-parent:b-user', {
    type: 1,
    bubbleId: 'b-user',
    text: 'what failed',
    createdAt: '2024-03-09T12:00:00.000Z',
    workspaceUris: ['file:///work/proj'],
  });
  insert(dbPath, 'bubbleId:cmp-parent:b-empty', {
    type: 2,
    bubbleId: 'b-empty',
    text: '',
    createdAt: '2024-03-09T12:00:01.000Z',
    tokenCount: { inputTokens: 3, outputTokens: 1 },
  });
  insert(dbPath, 'bubbleId:cmp-parent:b-tool', {
    type: 2,
    bubbleId: 'b-tool',
    text: 'done',
    createdAt: '2024-03-09T12:00:02.000Z',
    modelInfo: { modelName: 'gpt-test' },
    tokenCount: { inputTokens: 8, outputTokens: 4 },
    toolFormerData: {
      toolCallId: 'call-1',
      name: 'read',
      status: 'completed',
      params: { path: '/work/proj/a.ts' },
      result: 'contents',
    },
  });
  insert(dbPath, 'composerData:cmp-child', {
    _v: 12,
    composerId: 'cmp-child',
    name: 'Child',
    createdAt: 1710000001000,
    fullConversationHeadersOnly: [{ bubbleId: 'b-child', type: 1 }],
  });
  insert(dbPath, 'bubbleId:cmp-child:b-child', {
    type: 1,
    bubbleId: 'b-child',
    text: 'child prompt',
    createdAt: '2024-03-09T12:01:00.000Z',
  });
}

function discover(provider, { cursors = new Map(), indexed = [], issues = [] } = {}) {
  return provider.discover({
    lastCursor: key => cursors.get(key) ?? null,
    indexedSessions: () => indexed,
    reportIncompleteInventory: issue => issues.push(issue),
  });
}

function drain(generator) {
  const values = [];
  let step = generator.next();
  while (!step.done) {
    values.push(step.value);
    step = generator.next();
  }
  return { values, cursor: step.value };
}

function providerFor(dbPath, home) {
  return createCursorProvider({
    rootDir: dbPath,
    homeDir: home,
    openDatabase: openCursorDatabaseWithNodeSqlite,
  });
}

test('discover() reads the default platform database and an explicit root', () => {
  const home = makeTempDir('cursor-roots-');
  const automatic = defaultCursorDatabasePath(home);
  initStore(automatic);
  insert(automatic, 'composerData:cmp-auto', {
    composerId: 'cmp-auto',
    name: 'Automatic',
    fullConversationHeadersOnly: [],
  });
  const automaticProvider = createCursorProvider({
    homeDir: home,
    openDatabase: openCursorDatabaseWithNodeSqlite,
  });
  const automaticUnits = discover(automaticProvider).filter(unit => unit.sessionId !== '');
  assert.equal(automaticUnits.length, 1);
  assert.equal(automaticUnits[0].key, cursorUnitKey(automatic, 'cmp-auto'));

  const configRoot = join(home, 'CursorConfig');
  const nested = join(configRoot, 'User', 'globalStorage', 'state.vscdb');
  assert.equal(resolveCursorDatabase(configRoot, home), nested);
  initStore(nested);
  insert(nested, 'composerData:cmp-nested', {
    composerId: 'cmp-nested',
    name: 'Nested',
    fullConversationHeadersOnly: [],
  });
  const nestedUnits = discover(createCursorProvider({
    rootDir: configRoot,
    homeDir: home,
    openDatabase: openCursorDatabaseWithNodeSqlite,
  })).filter(unit => unit.sessionId !== '');
  assert.equal(nestedUnits[0].key, cursorUnitKey(nested, 'cmp-nested'));

  const fileRoot = join(home, 'explicit', 'state.vscdb');
  assert.equal(resolveCursorDatabase(fileRoot, home), fileRoot);
});

test('discover() keeps indexed sessions when the Cursor database cannot be read', () => {
  const home = makeTempDir('cursor-busy-');
  const dbPath = join(home, 'state.vscdb');
  initStore(dbPath);
  const provider = createCursorProvider({
    rootDir: dbPath,
    homeDir: home,
    openDatabase: () => { throw new Error('busy'); },
  });
  const issues = [];
  const sessionId = cursorSessionId('cmp-parent', dbPath);
  const units = discover(provider, {
    issues,
    indexed: [{ sessionId, jsonlPath: cursorUnitKey(dbPath, 'cmp-parent') }],
  });
  assert.equal(units.length, 0);
  assert.equal(issues.length, 1);
  assert.match(issues[0].error, /busy or unreadable/);
});

test('Cursor bubbles project users, empty text, tools, subagents, and round-trip', () => {
  const home = makeTempDir('cursor-project-');
  const dbPath = join(home, 'User', 'globalStorage', 'state.vscdb');
  seedConversation(dbPath);
  const provider = providerFor(dbPath, home);
  const found = discover(provider);
  const sessions = found.filter(unit => unit.sessionId !== '');
  assert.equal(sessions.length, 2);
  const parent = sessions.find(unit => unit.sessionId === cursorSessionId('cmp-parent', dbPath));
  const child = sessions.find(unit => unit.sessionId === cursorSessionId('cmp-child', dbPath));
  assert.equal(parent.isSubagent, false);
  assert.equal(child.isSubagent, true);

  const projected = drain(provider.parse(parent, null));
  const messages = projected.values.filter(record => record.kind === 'message');
  assert.deepEqual(messages.map(message => message.role), ['user', 'assistant', 'assistant']);
  assert.equal(messages[1].text, null);
  assert.equal(messages[1].content_type, 'unknown');
  assert.equal(messages[1].input_tokens, 3);
  assert.equal(messages[2].model, 'gpt-test');
  assert.equal(messages[0].cwd, '/work/proj');
  const tool = projected.values.find(record => record.kind === 'tool_call');
  const result = projected.values.find(record => record.kind === 'tool_result');
  assert.equal(tool.name, 'read');
  assert.equal(tool.file_path, '/work/proj/a.ts');
  assert.equal(result.content, 'contents');
  assert.equal(result.is_error, 0);
  const subagent = projected.values.find(record => record.kind === 'subagent');
  assert.equal(subagent.agent_id, child.sessionId);
  assert.equal(subagent.agent_type, 'subagent');
  const childProjected = drain(provider.parse(child, null));
  assert.equal(childProjected.values.find(record => record.kind === 'message').is_sidechain, 1);

  const direct = assembleSessionDetail(projected.values);
  const index = new DatabaseSync(':memory:');
  index.exec(SCHEMA);
  persist(index, parent, provider.parse(parent, null));
  const sessionId = parent.sessionId;
  const reassembled = assembleSessionDetail({
    session: index.prepare('SELECT * FROM sessions WHERE id = ?').get(sessionId),
    messages: index.prepare('SELECT * FROM messages WHERE session_id = ? ORDER BY timestamp, rowid').all(sessionId),
    toolCalls: index.prepare('SELECT * FROM tool_calls WHERE session_id = ?').all(sessionId),
    toolResults: index.prepare('SELECT * FROM tool_results WHERE session_id = ?').all(sessionId),
    subagents: index.prepare('SELECT * FROM subagents WHERE session_id = ?').all(sessionId),
  });
  assert.deepEqual(direct, reassembled);

  const raw = provider.raw({
    source: 'cursor',
    messageUuid: messages[2].uuid,
    session: { jsonl_path: parent.key },
    agentId: null,
  });
  assert.equal(raw.messageText, 'done');

  const cursors = new Map(found.map(unit => [unit.key, drain(provider.parse(unit, null)).cursor]));
  assert.deepEqual(discover(provider, { cursors }), []);
});

test('project path falls back to workspace.json when bubbles have no workspace URI', () => {
  const home = makeTempDir('cursor-workspace-');
  const dbPath = join(home, 'User', 'globalStorage', 'state.vscdb');
  initStore(dbPath);
  const workspaceId = 'ws1234567890';
  const workspaceJson = join(home, 'User', 'workspaceStorage', workspaceId, 'workspace.json');
  mkdirSync(join(workspaceJson, '..'), { recursive: true });
  writeFileSync(workspaceJson, JSON.stringify({ folder: 'file:///work/fallback' }));
  insert(dbPath, 'composerData:cmp-ws', {
    composerId: 'cmp-ws',
    name: 'Workspace',
    workspaceIdentifier: { id: workspaceId },
    fullConversationHeadersOnly: [{ bubbleId: 'b-ws', type: 1 }],
  });
  insert(dbPath, 'bubbleId:cmp-ws:b-ws', {
    type: 1,
    bubbleId: 'b-ws',
    text: 'hello',
    createdAt: '2024-03-09T12:00:00.000Z',
  });
  const provider = providerFor(dbPath, home);
  const unit = discover(provider).find(item => item.sessionId !== '');
  const message = drain(provider.parse(unit, null)).values.find(record => record.kind === 'message');
  assert.equal(message.cwd, '/work/fallback');
});

test('a changed composer is reindexed while an unchanged sibling is skipped', () => {
  const home = makeTempDir('cursor-incremental-');
  const dbPath = join(home, 'state.vscdb');
  seedConversation(dbPath);
  const provider = providerFor(dbPath, home);
  const first = discover(provider);
  const sessions = first.filter(unit => unit.sessionId !== '');
  assert.equal(sessions.length, 2);
  const cursors = new Map(first.map(unit => [unit.key, drain(provider.parse(unit, null)).cursor]));
  assert.deepEqual(discover(provider, { cursors }).filter(unit => unit.sessionId !== ''), []);

  insert(dbPath, 'bubbleId:cmp-parent:b-user', {
    type: 1,
    bubbleId: 'b-user',
    text: 'what failed after the edit',
    createdAt: '2024-03-09T12:00:00.000Z',
    workspaceUris: ['file:///work/proj'],
  });
  const again = discover(provider, { cursors });
  assert.deepEqual(
    again.filter(unit => unit.sessionId !== '').map(unit => unit.sessionId),
    [cursorSessionId('cmp-parent', dbPath)],
  );
  const childKey = cursorUnitKey(dbPath, 'cmp-child');
  assert.equal(again.find(unit => unit.key === childKey)?.meta?.kind, 'cursor-stamp');
  for (const unit of again) cursors.set(unit.key, drain(provider.parse(unit, null)).cursor);
  assert.deepEqual(discover(provider, { cursors }).filter(unit => unit.sessionId !== ''), []);
});
