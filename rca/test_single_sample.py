#!/usr/bin/env python3
"""
简化版OpenRCA测试脚本
用于单样本测试，记录完整的执行流程
"""

import os
import sys
import json
import argparse
from datetime import datetime
from loguru import logger
import pandas as pd
import signal

# 添加项目根目录到路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from main.evaluate import evaluate
from rca.api_router import configs
from rca.baseline.rca_agent.rca_agent import RCA_Agent
import rca.baseline.rca_agent.prompt.agent_prompt as ap

def handler(signum, frame):
    raise TimeoutError("Loop execution exceeded the time limit")

def create_detailed_logger(log_file):
    """创建详细的日志记录器"""
    logger.remove()  # 移除默认处理器
    
    # 控制台输出
    logger.add(sys.stdout, 
               colorize=True, 
               format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
               level="INFO")
    
    # 文件输出 - 详细日志
    logger.add(log_file,
               format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
               level="DEBUG",
               rotation="1 MB",
               retention="1 day")
    
    return logger

def test_single_sample(dataset="Market/cloudbed-1", sample_idx=0, output_dir="test_output", 
                      timeout=600, controller_max_step=25, controller_max_turn=5, temperature=0.0, no_eval=False):
    """
    测试单个样本的完整流程
    
    Args:
        dataset: 数据集名称
        sample_idx: 样本索引
        output_dir: 输出目录
        timeout: 超时时间（秒）
        controller_max_step: 控制器最大步数
        controller_max_turn: 控制器最大轮次
    """
    
    # 创建输出目录
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    test_dir = os.path.join(output_dir, f"test_{dataset}_{sample_idx}_{timestamp}")
    os.makedirs(test_dir, exist_ok=True)
    
    # 设置日志文件
    log_file = os.path.join(test_dir, "detailed_execution.log")
    logger_obj = create_detailed_logger(log_file)
    
    logger_obj.info("="*80)
    logger_obj.info("开始单样本测试")
    logger_obj.info(f"数据集: {dataset}")
    logger_obj.info(f"样本索引: {sample_idx}")
    logger_obj.info(f"输出目录: {test_dir}")
    logger_obj.info(f"超时时间: {timeout}秒")
    logger_obj.info(f"控制器最大步数: {controller_max_step}")
    logger_obj.info(f"控制器最大轮次: {controller_max_turn}")
    logger_obj.info("="*80)
    
    try:
        # 1. 导入数据集特定的提示词
        logger_obj.info("步骤1: 导入数据集特定的提示词")
        if dataset == "Telecom":
            import rca.baseline.rca_agent.prompt.basic_prompt_Telecom as bp
        elif dataset == "Bank":
            import rca.baseline.rca_agent.prompt.basic_prompt_Bank as bp
        elif dataset == "Market/cloudbed-1" or dataset == "Market/cloudbed-2":
            import rca.baseline.rca_agent.prompt.basic_prompt_Market as bp
        elif dataset == "phaseone":
            import rca.baseline.rca_agent.prompt.basic_prompt_phaseone as bp
        elif dataset == "YourDomain":  # 新增支持
            import rca.baseline.rca_agent.prompt.basic_prompt_YourDomain as bp
        else:
            raise ValueError(f"不支持的数据集: {dataset}")
        
        logger_obj.info(f"成功导入 {dataset} 的提示词模板")
        
        # 2. 加载数据
        logger_obj.info("步骤2: 加载数据文件")
        inst_file = f"dataset/{dataset}/query.csv"
        gt_file = f"dataset/{dataset}/record.csv"
        
        if not os.path.exists(inst_file) or not os.path.exists(gt_file):
            raise FileNotFoundError(f"数据文件不存在: {inst_file} 或 {gt_file}")
        
        instruct_data = pd.read_csv(inst_file)
        gt_data = pd.read_csv(gt_file)
        
        logger_obj.info(f"查询数据形状: {instruct_data.shape}")
        logger_obj.info(f"标准答案数据形状: {gt_data.shape}")
        
        # 3. 获取指定样本
        if sample_idx >= len(instruct_data):
            raise ValueError(f"样本索引 {sample_idx} 超出范围 (0-{len(instruct_data)-1})")
        
        row = instruct_data.iloc[sample_idx]
        instruction = row["instruction"]
        task_index = row["task_index"]
        scoring_points = row["scoring_points"]
        
        logger_obj.info(f"任务索引: {task_index}")
        logger_obj.info(f"问题: {instruction}")
        logger_obj.info(f"评分点: {scoring_points}")
        
        # 4. 设置超时控制
        signal.signal(signal.SIGALRM, handler)
        
        # 5. 执行RCA代理
        logger_obj.info("步骤3: 创建并执行RCA代理")
        logger_obj.info(f"使用模型: {configs['MODEL']}")
        logger_obj.info(f"控制器参数: max_step={controller_max_step}, max_turn={controller_max_turn}")
        
        signal.alarm(timeout)
        
        agent = RCA_Agent(ap, bp)
        prediction, trajectory, prompt = agent.run(
            instruction, 
            logger_obj, 
            max_step=controller_max_step,    # 使用输入参数
            max_turn=controller_max_turn,     # 使用输入参数
            temperature=temperature,
            debug=True
        )
        
        signal.alarm(0)  # 取消超时
        
        # 6. 保存详细结果
        logger_obj.info("步骤4: 保存执行结果")
        
        # 保存轨迹
        trajectory_file = os.path.join(test_dir, "trajectory.json")
        with open(trajectory_file, 'w', encoding='utf-8') as f:
            json.dump(trajectory, f, ensure_ascii=False, indent=2)
        logger_obj.info(f"轨迹已保存到: {trajectory_file}")
        
        # 保存完整对话历史
        prompt_file = os.path.join(test_dir, "full_conversation.json")
        with open(prompt_file, 'w', encoding='utf-8') as f:
            json.dump({"messages": prompt}, f, ensure_ascii=False, indent=2)
        logger_obj.info(f"完整对话已保存到: {prompt_file}")
        
        # 保存预测结果
        prediction_file = os.path.join(test_dir, "prediction.txt")
        with open(prediction_file, 'w', encoding='utf-8') as f:
            f.write(f"预测结果:\n{prediction}\n")
        logger_obj.info(f"预测结果已保存到: {prediction_file}")
        
        # 7. 评估结果
        if not no_eval:
            logger_obj.info("步骤5: 评估预测结果")
            if isinstance(prediction, bytes):
                prediction = prediction.decode('utf-8')
            passed_criteria, failed_criteria, score = evaluate(prediction, scoring_points)
            # 保存评估结果
            eval_result = {
                "task_index": task_index,
                "instruction": instruction,
                "prediction": prediction,
                "scoring_points": scoring_points,
                "passed_criteria": passed_criteria,
                "failed_criteria": failed_criteria,
                "score": score,
                "ground_truth": gt_data.iloc[sample_idx].to_dict(),
                "execution_params": {
                    "timeout": timeout,
                    "controller_max_step": controller_max_step,
                    "controller_max_turn": controller_max_turn,
                    "model": configs['MODEL']
                }
            }
            eval_file = os.path.join(test_dir, "evaluation_result.json")
            with open(eval_file, 'w', encoding='utf-8') as f:
                json.dump(eval_result, f, ensure_ascii=False, indent=2)
        
        # 8. 生成总结报告
        logger_obj.info("步骤6: 生成总结报告")
        summary_file = os.path.join(test_dir, "summary.txt")
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("OpenRCA 单样本测试总结报告\n")
            f.write("="*80 + "\n\n")
            f.write(f"测试时间: {timestamp}\n")
            f.write(f"数据集: {dataset}\n")
            f.write(f"样本索引: {sample_idx}\n")
            f.write(f"任务索引: {task_index}\n")
            f.write(f"使用模型: {configs['MODEL']}\n")
            f.write(f"超时时间: {timeout}秒\n")
            f.write(f"控制器最大步数: {controller_max_step}\n")
            f.write(f"控制器最大轮次: {controller_max_turn}\n\n")
            
            f.write("问题描述:\n")
            f.write(f"{instruction}\n\n")
            
            f.write("评分标准:\n")
            f.write(f"{scoring_points}\n\n")
            
            f.write("预测结果:\n")
            f.write(f"{prediction}\n\n")
            
            f.write("评估结果:\n")
            if not no_eval:
                f.write(f"通过标准: {passed_criteria}\n")
                f.write(f"失败标准: {failed_criteria}\n")
                f.write(f"最终分数: {score}\n\n")
            else:
                f.write("评估已跳过。\n\n")
            
            f.write("标准答案:\n")
            for col in gt_data.columns:
                if col != 'description':
                    f.write(f"{col}: {gt_data.iloc[sample_idx][col]}\n")
            f.write("\n")
            
            f.write("执行轨迹:\n")
            f.write(f"总步数: {len(trajectory)}\n")
            for i, step in enumerate(trajectory, 1):
                f.write(f"\n步骤 {i}:\n")
                f.write(f"代码:\n{step['code']}\n")
                f.write(f"结果:\n{step['result']}\n")
                f.write("-"*40 + "\n")
        
        logger_obj.info(f"总结报告已保存到: {summary_file}")
        
        # 9. 输出最终结果
        logger_obj.info("="*80)
        logger_obj.info("测试完成!")
        logger_obj.info(f"预测结果: {prediction}")
        logger_obj.info(f"评估分数: {score}")
        logger_obj.info(f"通过标准: {passed_criteria}")
        logger_obj.info(f"失败标准: {failed_criteria}")
        logger_obj.info(f"所有文件已保存到: {test_dir}")
        logger_obj.info("="*80)
        
        return {
            "success": True,
            "score": score,
            "prediction": prediction,
            "output_dir": test_dir,
            "execution_params": {
                "timeout": timeout,
                "controller_max_step": controller_max_step,
                "controller_max_turn": controller_max_turn
            }
        }
        
    except TimeoutError:
        logger_obj.error("执行超时!")
        return {"success": False, "error": "timeout"}
    except Exception as e:
        logger_obj.error(f"执行出错: {str(e)}")
        return {"success": False, "error": str(e)}

def main():
    parser = argparse.ArgumentParser(description="OpenRCA单样本测试脚本")
    parser.add_argument("--dataset", type=str, default="Market/cloudbed-1", 
                       help="数据集名称 (Market/cloudbed-1, Market/cloudbed-2, Bank, Telecom, YourDomain)")
    parser.add_argument("--sample_idx", type=int, default=0, 
                       help="要测试的样本索引")
    parser.add_argument("--output_dir", type=str, default="test_output", 
                       help="输出目录")
    parser.add_argument("--timeout", type=int, default=600, 
                       help="超时时间（秒），默认600秒")
    parser.add_argument("--controller_max_step", type=int, default=25, 
                       help="控制器最大步数，默认25")
    parser.add_argument("--controller_max_turn", type=int, default=5, 
                       help="控制器最大轮次，默认5")
    parser.add_argument("--temperature", type=float, default=0.3, 
                       help="温度，默认0.0")
    parser.add_argument("--no_eval", action="store_true", help="If set, do not perform evaluation/scoring, only save prediction and intermediate results.")
    
    args = parser.parse_args()
    
    result = test_single_sample(
        dataset=args.dataset,
        sample_idx=args.sample_idx,
        output_dir=args.output_dir,
        timeout=args.timeout,
        controller_max_step=args.controller_max_step,
        controller_max_turn=args.controller_max_turn,
        temperature=args.temperature,
        no_eval=args.no_eval
    )
    
    if result["success"]:
        print(f"\n✅ 测试成功完成!")
        print(f"📊 评估分数: {result['score']}")
        print(f"⚙️  执行参数: 超时={result['execution_params']['timeout']}s, "
              f"最大步数={result['execution_params']['controller_max_step']}, "
              f"最大轮次={result['execution_params']['controller_max_turn']}")
        print(f"📁 结果保存在: {result['output_dir']}")
    else:
        print(f"\n❌ 测试失败: {result['error']}")

if __name__ == "__main__":
    main()