import { useCallback, useEffect, useState } from "react";

const STORAGE_KEY = "dbt-docs-v2:favorites";
const HOME_KEY = "dbt-docs-v2:home-project";

function read(): Set<string> {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return new Set();
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? new Set(parsed) : new Set();
  } catch {
    return new Set();
  }
}

export function useFavorites() {
  const [ids, setIds] = useState<Set<string>>(() => read());

  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify([...ids]));
    } catch {
      // Storage unavailable (e.g. private mode); favorites become session-only.
    }
  }, [ids]);

  const toggle = useCallback((id: string) => {
    setIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const has = useCallback((id: string) => ids.has(id), [ids]);

  return { ids, has, toggle };
}

/** Tracks whether the current project has been set as the user's "home"
 *  project. Reads from localStorage so the toggle persists. */
export function useHomeProject(projectName: string | undefined) {
  const [home, setHome] = useState<string | null>(() => {
    try {
      return localStorage.getItem(HOME_KEY);
    } catch {
      return null;
    }
  });

  const isHome = !!projectName && home === projectName;

  const toggle = useCallback(() => {
    if (!projectName) return;
    setHome((prev) => {
      const next = prev === projectName ? null : projectName;
      try {
        if (next) localStorage.setItem(HOME_KEY, next);
        else localStorage.removeItem(HOME_KEY);
      } catch {
        // storage unavailable
      }
      return next;
    });
  }, [projectName]);

  return { isHome, toggle };
}
