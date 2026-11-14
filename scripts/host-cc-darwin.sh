#!/usr/bin/env bash
# mrustc's host C compiler on macOS: its -O1 becomes -O0, inside its @response file too. Apple
# clang 21 at -O1 miscompiles std's Once (a null dereference in WaiterQueue's drop).
args=()
for a in "$@"; do
  if [[ $a == @* && -f ${a#@} ]]; then
    sed 's/"-O1"/"-O0"/g' "${a#@}" > "${a#@}.O0" && args+=("@${a#@}.O0")
  else
    args+=("${a/#-O1/-O0}")
  fi
done
exec cc "${args[@]}"
