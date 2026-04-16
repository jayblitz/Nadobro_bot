import { useCallback, useEffect, useState } from "react";

const STORAGE_KEY = "nadobro:favoritePerps";

function loadFavorites(): Set<string> {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const arr: unknown = JSON.parse(raw);
      if (Array.isArray(arr)) {
        return new Set(arr.filter((x): x is string => typeof x === "string"));
      }
    }
  } catch (e) {
    console.warn("nadobro:favoritePerps load failed", e);
  }
  return new Set();
}

export function useFavoritePerps() {
  const [favorites, setFavorites] = useState<Set<string>>(loadFavorites);

  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify([...favorites]));
    } catch (e) {
      console.warn("nadobro:favoritePerps save failed", e);
    }
  }, [favorites]);

  const toggle = useCallback((name: string) => {
    const key = name.toUpperCase();
    setFavorites((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

  const isFavorite = useCallback(
    (name: string) => favorites.has(name.toUpperCase()),
    [favorites],
  );

  return { favorites, toggle, isFavorite };
}
