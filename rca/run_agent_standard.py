import os
import sys
import json
import argparse
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
from main.evaluate import evaluate
from rca.api_router import configs

from datetime import datetime
from loguru import logger
from nbformat import v4 as nbf
import pandas as pd
import signal
from parallel import parallel_run_filelist
from tqdm import tqdm
from rca.baseline.rca_agent.rca_agent import RCA_Agent
from concurrent.futures import ProcessPoolExecutor, as_completed

def handler(signum, frame):
    raise TimeoutError("Loop execution exceeded the time limit")

def reach_max_retry(history):
    for item in history:
        if "Max try reached. Please check the history" in item:
            return True
    return False

def run_one_problem(args_tuple):
    uuid, instruction, unique_obs_path, args = args_tuple
    if args.ap_version == 'v1':
        import rca.baseline.rca_agent.prompt.agent_prompt_v1 as ap
    else:
        import rca.baseline.rca_agent.prompt.agent_prompt as ap
    dataset = args.dataset
    if dataset == "Telecom":
        import rca.baseline.rca_agent.prompt.basic_prompt_Telecom as bp
    elif dataset == "Bank":
        import rca.baseline.rca_agent.prompt.basic_prompt_Bank as bp
    elif dataset == "Market/cloudbed-1" or dataset == "Market/cloudbed-2":
        import rca.baseline.rca_agent.prompt.basic_prompt_Market as bp
    elif dataset == "phaseone":
        if args.bp_version == 'v1':
            import rca.baseline.rca_agent.prompt.basic_prompt_phaseone_v1 as bp
        else:
            import rca.baseline.rca_agent.prompt.basic_prompt_phaseone as bp
    logfile = f"{unique_obs_path}/history/{uuid}.log"
    dst_trajectory_file = f"{unique_obs_path}/trajectory/{uuid}.json"
    dst_prompt_file = f"{unique_obs_path}/prompt/{uuid}.json"
    dst_prediction_file = f"{unique_obs_path}/prediction/{uuid}.txt"
    logger.remove()
    logger.add(sys.stdout, colorize=True, enqueue=True, level="INFO")
    logger.add(logfile, colorize=True, enqueue=True, level="INFO")
    logger.debug('\n' + "#"*80 + f"\nuuid: {uuid}\n" + "#"*80)
    try:
        agent = RCA_Agent(ap, bp)
        prediction, trajectory, prompt = agent.run(instruction, 
                                                logger, 
                                                max_step=args.controller_max_step, 
                                                max_turn=args.controller_max_turn,
                                                temperature=args.temperature, debug=False, uuid=uuid)
        # 保存轨迹
        with open(dst_trajectory_file, 'w', encoding='utf-8') as f:
            json.dump(trajectory, f, ensure_ascii=False, indent=2)
        # 保存完整对话历史
        with open(dst_prompt_file, 'w', encoding='utf-8') as f:
            json.dump({"messages": prompt}, f, ensure_ascii=False, indent=2)
        # 保存预测结果
        with open(dst_prediction_file, 'w', encoding='utf-8') as f:
            f.write(prediction)
        logger.info(f"[no_eval] prediction, trajectory, prompt saved for {uuid}, at {unique_obs_path}/prediction/{uuid}.txt")
        return uuid
    except Exception as e:
        logger.error(f"Exception in uuid {uuid}: {e}")
        return None

def main(args, uid):
    dataset = args.dataset
    inst_file = f"dataset/{dataset}/query.csv"
    eval_file = f"test/result/{dataset}/agent-{args.tag}-{configs['MODEL'].split('/')[-1]}.csv"
    obs_path = f"test/monitor/{dataset}/agent-{args.tag}-{configs['MODEL'].split('/')[-1]}"
    unique_obs_path = f"{obs_path}/{uid}"
    if args.ap_version:
        unique_obs_path += f"-ap-{args.ap_version}"
    if args.bp_version:
        unique_obs_path += f"-bp-{args.bp_version}"
    for d in ["history", "prediction", "trajectory", "prompt"]:
        os.makedirs(f"{unique_obs_path}/{d}", exist_ok=True)
    if not os.path.exists(eval_file):
        os.makedirs(f"test/result/{dataset}", exist_ok=True)
    logger.info(f"Using dataset: {dataset}")
    logger.info(f"Using model: {configs['MODEL'].split('/')[-1]}")
    instruct_data_ls = []
    for idx, line in enumerate(open(inst_file)):
        if idx == 0:
            continue
        uuid, instruction = line.strip().split(",")[:2]
        dst_prediction_file = f"{unique_obs_path}/prediction/{uuid}.txt"
        dst_history_file = f"{unique_obs_path}/history/{uuid}.log"
        if os.path.exists(dst_prediction_file):
            if os.path.exists(dst_history_file):
                history = list(open(dst_history_file))
                if not reach_max_retry(history):
                    # logger.info(f"Prediction file {dst_prediction_file} already exists, skipping...")
                    continue
                else:
                    os.remove(dst_history_file)
                    os.remove(dst_prediction_file)
                    logger.info(f"Prediction file {dst_prediction_file} already exists, but max retry reached, rerun...") 
            else:
                os.remove(dst_prediction_file)     
        instruct_data_ls.append((uuid, instruction, unique_obs_path, args))
    print('instruct_data_ls', len(instruct_data_ls))
    # exit()
    max_workers = min(args.n_procs, os.cpu_count())
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(run_one_problem, arg) for arg in instruct_data_ls]
        for fut in as_completed(futures):
            try:
                result = fut.result()
                if result is not None:
                    logger.info(f"Task {result} finished.")
            except Exception as e:
                logger.error(f"Task failed: {e}")

if __name__ == "__main__":
    uid = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    uid = "rca"
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, default="phaseone")
    parser.add_argument("--controller_max_step", type=int, default=25)
    parser.add_argument("--controller_max_turn", type=int, default=5)
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--tag", type=str, default='rca')
    parser.add_argument("--n_procs", type=int, default=12)
    parser.add_argument("--ap_version", type=str, default='v1')
    parser.add_argument("--bp_version", type=str, default='v1')
    args = parser.parse_args()
    main(args, uid)
