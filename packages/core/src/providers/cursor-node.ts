// Copyright (C) 2026 tommy0103 and contributors.
// SPDX-License-Identifier: AGPL-3.0-only

import { DatabaseSync } from 'node:sqlite';

import type { CursorDatabaseOpener } from './cursor.ts';

/** CLI-side read-only Cursor database opener; Electron injects better-sqlite3 instead. */
export const openCursorDatabaseWithNodeSqlite: CursorDatabaseOpener = path => (
  new DatabaseSync(path, { readOnly: true, timeout: 500 })
);
