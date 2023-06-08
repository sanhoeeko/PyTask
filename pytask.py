import os
import random
import string
import threading
import time

import pandas as pd


def randString(length: int):
    return ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase + string.digits, k=length))


def autosave(result: str, cnt: int, thread=False):
    """
    :param result: text only
    :param cnt: only for single thread!!!
    :param thread: if you use multi thread, set it as True!!!
    :return:
    """
    filename = randString(10) + '.txt' if thread else str(cnt) + '.txt'
    with open(filename, 'a', encoding='utf-8') as a:
        a.write(result + '\n')


class Task:
    def __init__(self, taskname: str, objlist: list, action, savehandler=autosave, crashhandler=None, sleeptime=1):
        """
        :param taskname: the name of .csv pickle file
        :param objlist: objects must have str() method and the result is unique
        :param action: callable: action(obj:any)->result:any, is the task probably cause exception
        :param savehandler: callable: save(result:any, cnt:int)->None, save the result real-time. need a changable name
        :param crashhandler: callable|None: if there's any operation for the exception
        :param sleeptime: float(second): if the task need to pause for a while to avoid DDOS
        """
        self.taskname = taskname
        self.objlist = objlist
        self.action = action
        self.savehandler = savehandler
        self.crashhandler = crashhandler
        self.sleeptime = sleeptime

        self.df = None
        self.cnt = 0
        self.init()

    def filename(self):
        return self.taskname + '.csv'

    def init(self):
        # if it is not the first time to do the task
        if os.path.exists(self.filename()):
            self.df = pd.read_csv(self.filename())

        # if it is the first time
        else:
            df = pd.DataFrame(columns=['name', 'success', 'message'])
            for obj in self.objlist:
                df = df.append({'name': str(obj), 'success': False, 'message': None}, ignore_index=True)
            df.to_csv(self.filename(), index=False)
            self.init()

    def act(self):
        for i in range(len(self.df)):
            if self.df['success'][i]:
                continue
            else:
                obj = self.objlist[i]
                try:
                    result = self.action(obj)
                    if result is not None:
                        self.savehandler(result, i)
                    # if successfully saved:
                    print('### success:', i)
                    self.df.loc[i, 'success'] = True
                except Exception as e:
                    print('### fail:', i)
                    print(e)
                    if self.crashhandler:
                        self.crashhandler(e)

            time.sleep(self.sleeptime)

        self.cnt += 1
        self.df.to_csv(self.filename(), index=False)

    def act_in_list(self, lst: list[int]):
        for i in lst:
            obj = self.objlist[i]
            try:
                result = self.action(obj)
                if result is not None:
                    self.savehandler(result, i, True)
                # if successfully saved:
                print('### success:', i)
                self.df.loc[i, 'success'] = True
            except Exception as e:
                print('### fail:', i)
                print(e)
                if self.crashhandler:
                    self.crashhandler(e)

            time.sleep(self.sleeptime)

        self.cnt += 1
        self.df.to_csv(self.filename(), index=False)

    def allocate_task(self, num_thread):
        tasks = []
        for i in range(len(self.df)):
            if not self.df['success'][i]:
                tasks.append(i)
        task_len = len(tasks)
        if task_len < num_thread:
            num_thread = task_len
        task_per_thread = task_len // num_thread
        task_remainder = task_len % num_thread
        task_list = []
        start = 0
        for i in range(num_thread):
            if i < task_remainder:
                end = start + task_per_thread + 1
            else:
                end = start + task_per_thread
            task_list.append(tasks[start:end])
            start = end
        return task_list

    def mth_act(self, num_thread=4):
        threads = []
        task_list = self.allocate_task(num_thread)
        for i in range(num_thread):
            t = threading.Thread(target=self.act_in_list, args=(task_list[i],))
            threads.append(t)
            t.start()
            time.sleep(1)  # async visit
        for t in threads:
            t.join()
