# Repository workflow

- Use `main` as the only development branch, tracking `origin/main`.
- Work in the existing checkout. Do not create additional branches, worktrees,
  or development clones unless the user explicitly requests an exception.
- Before integrating or pushing, fetch the remote state and preserve all
  unrelated tracked and untracked work. Commit only the task's intended files.
- Keep recovery copies outside the repository as private archives or Git
  bundles, rather than backup branches.
- Delete a superseded branch only after verifying that all its commits are
  reachable from the pushed `main`. Do not rewrite published history.
