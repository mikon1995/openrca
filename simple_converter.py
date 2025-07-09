#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化的Parquet转CSV转换工具
用于将openrca数据集中的parquet文件转换为csv格式
"""

import os
import sys
from pathlib import Path

def convert_parquet_to_csv(parquet_path, csv_path):
    """转换单个parquet文件为csv"""
    try:
        import pandas as pd
        
        print(f"正在转换: {parquet_path}")
        
        # 读取parquet文件
        df = pd.read_parquet(parquet_path)
        
        if df.empty:
            print(f"  警告: 文件为空")
            return False
        
        # 创建输出目录
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        
        # 保存为csv
        df.to_csv(csv_path, index=False, encoding='utf-8')
        
        print(f"  转换成功: {csv_path}")
        print(f"  数据形状: {df.shape}")
        print(f"  列名: {list(df.columns)}")
        
        return True
        
    except ImportError:
        print("错误: 需要安装pandas库")
        print("请运行: pip install pandas pyarrow")
        return False
    except Exception as e:
        print(f"  转换失败: {e}")
        return False

def convert_directory(input_dir, output_dir):
    """转换目录下的所有parquet文件"""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    if not input_path.exists():
        print(f"错误: 输入目录不存在: {input_dir}")
        return
    
    # 创建输出目录
    output_path.mkdir(parents=True, exist_ok=True)
    
    # 查找所有parquet文件
    parquet_files = list(input_path.rglob("*.parquet"))
    
    if not parquet_files:
        print(f"未找到parquet文件: {input_dir}")
        return
    
    print(f"找到 {len(parquet_files)} 个parquet文件")
    
    success_count = 0
    failed_count = 0
    
    for parquet_file in parquet_files:
        # 生成输出路径
        relative_path = parquet_file.relative_to(input_path)
        csv_file = output_path / relative_path.with_suffix('.csv')
        
        if convert_parquet_to_csv(parquet_file, csv_file):
            success_count += 1
        else:
            failed_count += 1
    
    print(f"\n转换完成:")
    print(f"  成功: {success_count}")
    print(f"  失败: {failed_count}")
    print(f"  总计: {len(parquet_files)}")

def main():
    """主函数"""
    # 默认路径
    input_dir = "openrca/dataset/2025-06-06"
    output_dir = "openrca/dataset/2025-06-06-csv"
    
    # 检查命令行参数
    if len(sys.argv) > 1:
        input_dir = sys.argv[1]
    if len(sys.argv) > 2:
        output_dir = sys.argv[2]
    
    print("=" * 50)
    print("Parquet转CSV转换工具")
    print("=" * 50)
    print(f"输入目录: {input_dir}")
    print(f"输出目录: {output_dir}")
    print("=" * 50)
    
    # 转换所有数据类型
    data_types = ["metric", "trace", "log"]
    
    for data_type in data_types:
        type_input = os.path.join(input_dir, f"{data_type}-parquet")
        type_output = os.path.join(output_dir, f"{data_type}-csv")
        
        if os.path.exists(type_input):
            print(f"\n转换 {data_type} 数据...")
            convert_directory(type_input, type_output)
        else:
            print(f"\n跳过 {data_type} 数据 (目录不存在)")
    
    print("\n" + "=" * 50)
    print("转换完成!")
    print("=" * 50)

if __name__ == "__main__":
    main() 