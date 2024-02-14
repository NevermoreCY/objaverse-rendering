import glob
import json
import multiprocessing
import shutil
import subprocess
import time
from dataclasses import dataclass
from typing import Optional

import boto3
import tyro
import wandb
import sys
import os
@dataclass
class Args:

    job_num: int





if __name__ == "__main__":
    args = tyro.cli(Args)

    input_models_path = '/objaverse/objaverse-rendering/valid_paths_5.json'

    with open(input_models_path, "r") as f:
        model_paths = json.load(f)

    L = len(model_paths)
    interval = L // 32

    start_id = args.job_num * interval
    end_id = start_id + interval

    # ensure all on same stage
    # if args.job_num % 2 == 0:
    #     start_id = start_id + 6900

    if args.job_num == 32:
        end_id = L
    # 119389  , 7461

    model_paths = model_paths[start_id:end_id]

    dir_path =  '/objaverse/control3D/views_subdir' + str(args.job_num)
    os.system('mkdir ' + dir_path )
    c = 0
    for id in model_paths:
        path = '/objaverse/control3D/views_valid_5_30/' + id

        cmd = 'mv ' + path + " " + dir_path + '/' + id
        print('Run ' + cmd)
        print('\n ', c/L )
        os.system(cmd)
        c+=1

    cmd = 'zip -r ' + '/objaverse/control3D/views_subdir' + str(args.job_num) +'.zip ' +  dir_path
    print('\n\n\n zip cmd: ', cmd)
    os.system(cmd)















