# PyTask
 Good habit: save result when doing dangerous tasks.

## Usage

example:

```python
from pytask import Task
task = Task('screenshot',
            urls,
            screenshot_action,
            None,
            sleeptime=3)

task.init()
# task.act() # single thread
task.mth_act(4) # multi thread
```
