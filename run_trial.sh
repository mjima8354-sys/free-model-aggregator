#!/bin/bash
# Usage: run_trial.sh <trial-index 0|1|2> [main.py path]
set -e
KIT=/root/makefun/kit-fma/free-model-aggregator-01-kit
T=${1:-0}
BOT=${2:-/root/makefun/build/free-model-aggregator-01/main.py}
RUN=/tmp/fma-verify-$T; rm -rf $RUN; mkdir -p $RUN/state
cp -r $KIT/practice/trial-$T/input/env $RUN/env-data
MAKEFUN_STATE_DIR=$RUN/state MAKEFUN_ENV_DATA_DIR=$RUN/env-data MAKEFUN_ENV_HOST=127.0.0.1 MAKEFUN_ENV_PORT=1908$T node $KIT/program/env/server.mjs > $RUN/env.log 2>&1 &
ENV_PID=$!
sleep 1.5
MAKEFUN_ENV_URL=http://127.0.0.1:1908$T MAKEFUN_PROMPT_FILE=$KIT/practice/trial-$T/input/prompt.txt MAKEFUN_TRIAL_SEED=$T MAKEFUN_TRIAL_NOW=2026-11-15T00:00:00.000Z python3 $BOT > $RUN/bot.log 2>&1 &
BOT_PID=$!
for i in $(seq 1 120); do kill -0 $BOT_PID 2>/dev/null || break; sleep 1; done
kill -0 $BOT_PID 2>/dev/null && { echo BOT_STILL_RUNNING; kill -9 $BOT_PID; }
kill -TERM $ENV_PID 2>/dev/null || true; sleep 2
MAKEFUN_OUTPUT_DIR=$RUN/state/export MAKEFUN_REFERENCE_DIR=$KIT/practice/trial-$T/reference MAKEFUN_SCORES_DIR=$RUN/scores node $KIT/program/scorer/score.mjs || true
cat $RUN/scores/scores.json 2>/dev/null || echo NO_SCORE
