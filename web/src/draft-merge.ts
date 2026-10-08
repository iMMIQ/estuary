import type { NodeDraft } from "./types";

// Keep local edits and take remote changes to untouched fields. Arrays are
// atomic (model/header rows), so simultaneous edits are explicitly reported.
export function mergeDraft(base: NodeDraft, local: NodeDraft, remote: NodeDraft) {
  const conflicts: string[] = [];
  const same = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b);
  const object = (value: unknown): value is Record<string, unknown> =>
    value !== null && typeof value === "object" && !Array.isArray(value);
  const merge = (before: unknown, mine: unknown, theirs: unknown, path: string): unknown => {
    if (same(before, mine)) return theirs;
    if (same(before, theirs) || same(mine, theirs)) return mine;
    if (object(before) && object(mine) && object(theirs)) {
      return Object.fromEntries(
        [...new Set([...Object.keys(before), ...Object.keys(mine), ...Object.keys(theirs)])].map(
          (key) => [key, merge(before[key], mine[key], theirs[key], path ? `${path}.${key}` : key)],
        ),
      );
    }
    conflicts.push(path);
    return mine;
  };
  return { draft: merge(base, local, remote, "") as NodeDraft, conflicts };
}
