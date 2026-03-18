# Store-level ignore patterns (one per line, fnmatch syntax)
# Applied to all directory walks and watches, in addition to .gitignore
# Edit with: keep get .ignore / keep put .ignore

# Build output
*.min.js
*.min.css
*.bundle.js
*.chunk.js
*.map

# Package lock files
package-lock.json
yarn.lock
pnpm-lock.yaml

# Python bytecode
*.pyc
__pycache__/*

# Build directories
dist/*
build/*
.next/*
.nuxt/*
.output/*

# Binary artifacts
*.wasm
*.so
*.dylib
*.dll
