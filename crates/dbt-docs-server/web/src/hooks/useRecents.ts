import { useCallback, useEffect, useState } from "react";

const STORAGE_KEY = "dbt-docs-v2:recents";
const MAX_ITEMS = 20;

function read(): string[] {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.filter((x) => typeof x === "string") : [];
  } catch {
    return [];
  }
}

export function useRecents() {
  const [ids, setIds] = useState<string[]>(() => read());

  useEffect(() => {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(ids));
    } catch {
      // sessionStorage unavailable; keep in memory only.
    }
  }, [ids]);

  const push = useCallback((id: string) => {
    setIds((prev) => {
      const filtered = prev.filter((x) => x !== id);
      return [id, ...filtered].slice(0, MAX_ITEMS);
    });
  }, []);

  return { ids, push };
}
