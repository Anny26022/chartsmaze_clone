import { useCallback, useState, type Dispatch, type SetStateAction } from 'react';

const read = <T,>(key: string, fallback: T): T => {
  try {
    const stored = window.localStorage.getItem(key);
    return stored == null ? fallback : JSON.parse(stored) as T;
  } catch {
    return fallback;
  }
};

/** Stores small user choices only; market data stays in immutable snapshots. */
export const useLocalStorageState = <T,>(key: string, fallback: T): [T, Dispatch<SetStateAction<T>>] => {
  const [value, setValue] = useState<T>(() => typeof window === 'undefined' ? fallback : read(key, fallback));
  const setStoredValue = useCallback<Dispatch<SetStateAction<T>>>((next) => {
    setValue(previous => {
      const resolved = typeof next === 'function' ? (next as (current: T) => T)(previous) : next;
      try {
        window.localStorage.setItem(key, JSON.stringify(resolved));
      } catch {
        // Browser privacy settings or quota must not stop a screen from running.
      }
      return resolved;
    });
  }, [key]);
  return [value, setStoredValue];
};
