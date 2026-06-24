@echo off
copy /Y ..\lc_worker\lc_worker.js index.js
node inject_hlp.js
node index.js
