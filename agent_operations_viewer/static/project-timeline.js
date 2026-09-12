(() => {
  const timeline = document.querySelector('[data-project-timeline]');
  if (!timeline) return;
  const items = timeline.querySelector('[data-timeline-items]');
  const pagination = timeline.querySelector('[data-timeline-pagination]');
  const status = timeline.querySelector('[data-timeline-status]');
  let loading = false;

  // Restore appended turns when returning from a session without the browser's
  // back/forward cache. The snapshot belongs to this history entry only.
  const saved = history.state?.projectTimeline;
  if (performance.getEntriesByType('navigation')[0]?.type === 'back_forward' && saved) {
    items.innerHTML = saved.items;
    pagination.innerHTML = saved.pagination;
    requestAnimationFrame(() => window.scrollTo(0, saved.scrollY));
  }
  window.addEventListener('pagehide', () => {
    try {
      history.replaceState({ ...history.state, projectTimeline: {
        items: items.innerHTML,
        pagination: pagination.innerHTML,
        scrollY: window.scrollY,
      } }, '');
    } catch (_) {
      // Browser history limits must not prevent normal session navigation.
    }
  });

  pagination.addEventListener('click', async (event) => {
    const link = event.target.closest('[data-load-older]');
    if (!link || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    if (loading) return;
    loading = true;
    link.setAttribute('aria-disabled', 'true');
    timeline.setAttribute('aria-busy', 'true');
    status.hidden = false;
    status.textContent = 'Loading older turns…';
    try {
      const response = await fetch(link.href);
      if (!response.ok || response.redirected) throw new Error('Unable to load turns');
      const document = new DOMParser().parseFromString(await response.text(), 'text/html');
      const nextItems = document.querySelector('[data-timeline-items]');
      const nextPagination = document.querySelector('[data-timeline-pagination]');
      if (!nextItems || !nextPagination) throw new Error('Timeline unavailable');
      const existing = new Set(Array.from(items.children, (item) => item.dataset.timelineTurn));
      const additions = Array.from(nextItems.querySelectorAll('[data-timeline-turn]'))
        .filter((item) => !existing.has(item.dataset.timelineTurn));
      items.append(...additions);
      pagination.replaceChildren(...nextPagination.childNodes);
      status.textContent = `${additions.length} older turns loaded.`;
      // Move keyboard focus into the newly loaded history without jumping the
      // viewport away from the reader's position.
      additions[0]?.querySelector('[data-turn-session-link]')?.focus({ preventScroll: true });
    } catch (_) {
      link.removeAttribute('aria-disabled');
      status.textContent = 'Couldn’t load older turns. Try again.';
    } finally {
      loading = false;
      timeline.removeAttribute('aria-busy');
    }
  });
})();
