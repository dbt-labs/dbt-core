import { useCallback, useEffect, useState } from "react";

type Theme = "dark" | "light";
const STORAGE_KEY = "dbt-docs-v2:theme";
const DEFAULT: Theme = "dark";

function read(): Theme {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw === "light" || raw === "dark" ? raw : DEFAULT;
  } catch {
    return DEFAULT;
  }
}

export function useTheme() {
  const [theme, setTheme] = useState<Theme>(() => read());

  useEffect(() => {
    const root = document.documentElement;
    root.classList.toggle("dark", theme === "dark");
    root.classList.toggle("light", theme === "light");
    try {
      localStorage.setItem(STORAGE_KEY, theme);
    } catch {
      // Storage unavailable; in-memory only.
    }
  }, [theme]);

  const toggle = useCallback(() => {
    setTheme((t) => (t === "dark" ? "light" : "dark"));
  }, []);

  return { theme, toggle, setTheme };
}
