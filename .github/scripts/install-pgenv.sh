#!/usr/bin/env bash
set -euo pipefail

pg_version="${1:-18.4}"
pgenv_home="${PGENV_ROOT:-${HOME}/.pgenv}"

if [[ ! -d "${pgenv_home}/.git" ]]; then
  git clone --branch v1.4.3 --depth 1 https://github.com/theory/pgenv.git \
    "${pgenv_home}"
fi

export PATH="${pgenv_home}/bin:${pgenv_home}/pgsql/bin:${PATH}"

if [[ "$(uname -s)" == "Darwin" ]]; then
  brew_prefix="$(brew --prefix)"
  export PATH="${brew_prefix}/opt/bison/bin:${brew_prefix}/opt/flex/bin:${PATH}"
  export CPPFLAGS="-I${brew_prefix}/opt/icu4c/include -I${brew_prefix}/opt/openssl@3/include -I${brew_prefix}/opt/readline/include -I${brew_prefix}/opt/zlib/include ${CPPFLAGS:-}"
  export LDFLAGS="-L${brew_prefix}/opt/icu4c/lib -L${brew_prefix}/opt/openssl@3/lib -L${brew_prefix}/opt/readline/lib -L${brew_prefix}/opt/zlib/lib ${LDFLAGS:-}"
  export PKG_CONFIG_PATH="${brew_prefix}/opt/icu4c/lib/pkgconfig:${brew_prefix}/opt/openssl@3/lib/pkgconfig:${brew_prefix}/opt/readline/lib/pkgconfig:${brew_prefix}/opt/zlib/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
fi

if [[ ! -x "${pgenv_home}/pgsql-${pg_version}/bin/pg_config" ]]; then
  MAKEFLAGS="${MAKEFLAGS:--j $(getconf _NPROCESSORS_ONLN)}" pgenv build "${pg_version}"
fi

pgenv switch "${pg_version}"
pg_config --version
