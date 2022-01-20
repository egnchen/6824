# 6824
My MIT 6.824 course labs solution.

Few things to notice:
* When running lab1 `test-mr.sh` on MacOS, you should use a newer version of `bash` since the one shipped with MacOS is too old(version 3.2, 2007) and doesn't support `wait -n`.
  * Solution: install it by `brew install bash`, or use zsh instead: `zsh test-mr.sh`
