This branch is experimental to investigate using async io.

It is abandoned for several reasons:

1. The async code is too complex
2. Current implementation still requires a reader thread
3. The performance was not better for a reasonable number of subscribers/publishers. I expect this would be different with 1000s.
4. Most importantly, Project Loom! making all of this async nonsense obsolete. See the 'virtual_threads' branch. 