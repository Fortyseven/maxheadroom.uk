# AGENTS.md

This file provides essential context and guidelines for agentic coding agents operating in this repository.

## Project Overview

The repository contains two primary components:
1. **Static Site (`site/`)**: A website built using **Eleventy (11ty)**, **Nunjucks**, and **SASS/SCSS**.
2. **Extraction Tool (`extraction/`)**: A Python-based tool managed with `uv`.

## Development Commands

### Static Site (`site/`)
Commands are managed via `npm`.

- **Development Mode**: `npm run dev`
  - Starts `sass:watch` and `eleventy:serve` in parallel.
- **Production Build**: `npm run build`
  - Runs the Eleventy build process.

### Placeholder Site (`placeholder/`)
This is a placeholder site while the main site is under development. It does not have specific commands but can be served using any static file server (e.g., `python -m http.server`). Ignore this directory for development purposes.

### Extraction Tool (`extraction/`)
Commands are managed via `uv`.

- **Run Scripts**: `uv run <script_name>.py`

## Testing & Linting

- **Testing**: No automated test suites (e.g., Jest, Pytest) are currently configured.
- **Linting**: No explicit linting commands are defined in `package.json` or `pyproject.toml`.
  - *Recommendation*: When adding new features, consider implementing `ruff` for Python and `eslint` for JS/TS.

## Code Style & Conventions

### Static Site (`site/`)
- **Templating**: Use **Nunjucks (`.njk`)** for componentization. Utilize macros (e.g., in `src/_includes/macros.njk`) to maintain DRY principles.
- **Content**: Use **Markdown (`.md`)** with YAML Frontmatter for content.
- **Styling**: Use **SASS/SCSS**. Styles are located in `src/_scss/` and compiled to `src/css/style.css`.
- **Directory Structure**:
  - Layouts: `src/_includes/layouts/`
  - Content Categorization: Use directories like `infobanks/` to organize content by show or network.
  - Assets: Images and fonts are served from the `public/` directory.

### Extraction Tool (`extraction/`)
- **Language**: Python 3.11+.
- **Environment**: Use `uv` for dependency management and script execution.
- **Style**: Follow standard Python PEP 8 conventions.

## Instructions for Agents

1. **Context Awareness**: Before making changes to `site/`, understand the relationship between Markdown content, Nunjucks templates, and SCSS styles.
2. **Minimal Disruption**: When modifying templates, ensure you do not break existing Nunjucks macros or layouts.
3. **Incremental Improvements**: If you implement testing or linting, propose it first or follow existing patterns if they emerge.
