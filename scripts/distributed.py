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

# C = 0


@dataclass
class Args:
    workers_per_gpu: int
    """number of workers per gpu"""

    input_models_path: str
    """Path to a json file containing a list of 3D object files"""

    out_dir: str
    """Path to a json file containing a list of 3D object files"""

    job_num: int

    upload_to_s3: bool = False
    """Whether to upload the rendered images to S3"""

    log_to_wandb: bool = False
    """Whether to log the progress to wandb"""

    num_gpus: int = -1
    """number of gpus to use. -1 means all available gpus"""




def worker(
    queue: multiprocessing.JoinableQueue,
    count: multiprocessing.Value,
    gpu: int,
    s3: Optional[boto3.client],
    out_dir: str,
) -> None:
    C=0
    while True:
        item = queue.get()
        if item is None:
            break

        # Perform some operation on the item
        print('\n\n\n\n\n\n\n\n\n' , C ,item, )
        C+=1
        command = (
            f" blender-3.2.2-linux-x64/blender -b -P scripts/blender_script_MVD.py --"
            f" --object_path {item}"
            f" --output_dir {out_dir}"
        )
        subprocess.run(command, shell=True)

        if args.upload_to_s3:
            if item.startswith("http"):
                uid = item.split("/")[-1].split(".")[0]
                for f in glob.glob(f"views/{uid}/*"):
                    s3.upload_file(
                        f, "objaverse-images", f"{uid}/{f.split('/')[-1]}"
                    )
            # remove the views/uid directory
            shutil.rmtree(f"views/{uid}")

        with count.get_lock():
            count.value += 1

        queue.task_done()


if __name__ == "__main__":
    args = tyro.cli(Args)

    s3 = boto3.client("s3") if args.upload_to_s3 else None
    queue = multiprocessing.JoinableQueue()
    count = multiprocessing.Value("i", 0)

    out_dir = args.out_dir

    if args.log_to_wandb:
        wandb.init(project="objaverse-rendering", entity="prior-ai2")

    # Start worker processes on each of the GPUs
    for gpu_i in range(args.num_gpus):
        for worker_i in range(args.workers_per_gpu):
            worker_i = gpu_i * args.workers_per_gpu + worker_i
            process = multiprocessing.Process(
                target=worker, args=(queue, count, gpu_i, s3, out_dir)
            )
            process.daemon = True
            process.start()

    # Add items to the queue
    with open(args.input_models_path, "r") as f:
        model_paths = json.load(f)

    # print('\n\n\n\n\n model paths', type(model_paths), len(model_paths))
    # L = len(model_paths)
    # interval = L // 32
    #
    #
    # start_id = args.job_num * interval
    # end_id = start_id + interval
    #
    # if args.job_num == 32:
    #     end_id = L
    #
    # model_paths = model_paths[start_id:end_id]
    # print('\n\n\n\n curent start id is ', start_id, ' end id is ', end_id, ' interval is ', len(model_paths))




    for item in model_paths:
        queue.put(item)

    # Wait for all tasks to be completed
    queue.join()

    # Add sentinels to the queue to stop the worker processes
    for i in range(args.num_gpus * args.workers_per_gpu):
        queue.put(None)
