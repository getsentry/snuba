// Set a max history size so we don't slow the page down with 100s of queries
const HISTORY_SIZE = 5;

export function setRecentHistory(key: string, query: any) {
  const storageKey = `${key}.history`
  try {
    if (query === undefined) {
      return;
    }
    const recentHistory = localStorage.getItem(storageKey);
    let queries: any[] = []
    if (recentHistory) {
      queries = JSON.parse(recentHistory);
    }
    if (!queries.includes(query)) {
      queries.unshift(query);
    }
    if (queries.length > HISTORY_SIZE) {
      queries.pop();
    }
    localStorage.setItem(storageKey, JSON.stringify(queries));
  } catch {
    // If we error lets wipe history so we don't constantly loop in error land
    localStorage.removeItem(storageKey);
  }
}

export function getRecentHistory(key: string) {
  const storageKey = `${key}.history`
  const recentHistory = localStorage.getItem(storageKey)
  if (recentHistory) {
    try {
      return JSON.parse(recentHistory);
    } catch {
      return [];
    }
  } else {
    return [];
  }
}
