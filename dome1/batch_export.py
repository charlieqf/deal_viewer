# coding=utf-8
"""
批量导出 Trust 数据
用法: py batch_export.py trustids.txt
      (需要先设置 py 别名指向 E:/Program Files/Python36/python.exe)
"""
import sys
import os
import subprocess
import datetime
import re

# 与 exporttrustdata.ps1 保持一致的 Python 路径
PYTHON_EXE = r"E:/Program Files/Python36/python.exe"

# 单个导出任务的超时时间（秒）
EXPORT_TIMEOUT = 600  # 10分钟

def log(message, log_file=None):
    """带时间戳的日志输出"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = "[{0}] {1}".format(timestamp, message)
    print(log_line)
    # 同时写入日志文件
    if log_file:
        try:
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(log_line + '\n')
        except:
            pass

def validate_trust_id(trust_id):
    """验证 TrustID 是否为有效的数字"""
    # 允许纯数字
    return trust_id.isdigit()

def batch_export(txt_file):
    """
    读取 txt 文件中的 TrustID 列表，依次调用 exporttrustdata.py
    
    Args:
        txt_file: txt 文件路径，每行一个 TrustID
    """
    # 生成日志文件路径
    log_dir = os.path.dirname(os.path.abspath(txt_file)) or '.'
    log_filename = "batch_export_{0}.log".format(
        datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    log_file = os.path.join(log_dir, log_filename)
    
    # 检查 Python 路径
    if not os.path.exists(PYTHON_EXE):
        log("错误: Python 解释器不存在 - {0}".format(PYTHON_EXE), log_file)
        log("请检查 PYTHON_EXE 配置是否正确", log_file)
        return
    
    if not os.path.exists(txt_file):
        log("错误: 文件不存在 - {0}".format(txt_file), log_file)
        return
    
    # 获取当前脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    export_script = os.path.join(script_dir, "exporttrustdata.py")
    
    if not os.path.exists(export_script):
        log("错误: 导出脚本不存在 - {0}".format(export_script), log_file)
        return
    
    # 读取 TrustID 列表
    # 读取 TrustID 列表
    raw_lines = []
    try:
        with open(txt_file, 'r', encoding='utf-8') as f:
            raw_lines = [line.strip() for line in f if line.strip()]
    except UnicodeDecodeError:
        log("警告: UTF-8 解码失败，尝试使用 GBK 编码读取...", log_file)
        try:
            with open(txt_file, 'r', encoding='gbk') as f:
                raw_lines = [line.strip() for line in f if line.strip()]
        except Exception as e:
            log("错误: 无法读取文件，请检查编码 - {0}".format(e), log_file)
            return
    
    # 过滤和验证 TrustID
    trust_ids = []
    skipped_lines = []
    for line in raw_lines:
        # 跳过注释行
        if line.startswith('#') or line.startswith('//'):
            skipped_lines.append(line)
            continue
        if validate_trust_id(line):
            trust_ids.append(line)
        else:
            skipped_lines.append(line)
            log("警告: 跳过无效的 TrustID - {0}".format(line), log_file)
    
    total = len(trust_ids)
    
    if total == 0:
        log("错误: 未找到有效的 TrustID", log_file)
        return
    
    log("共读取到 {0} 个有效 TrustID".format(total), log_file)
    if skipped_lines:
        log("跳过了 {0} 行无效内容".format(len(skipped_lines)), log_file)
    log("日志文件: {0}".format(log_file), log_file)
    log("=" * 50, log_file)
    
    success_count = 0
    failed_ids = []
    
    for index, trust_id in enumerate(trust_ids, 1):
        log("[{0}/{1}] 正在导出 TrustID: {2}".format(index, total, trust_id), log_file)
        
        try:
            # 调用 exporttrustdata.py，使用指定的 Python 路径
            # 调用 exporttrustdata.py，使用指定的 Python 路径
            # Python 3.6 兼容性修改: 使用 stdout/stderr 代替 capture_output, universal_newlines 代替 text
            result = subprocess.run(
                [PYTHON_EXE, export_script, trust_id],
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                universal_newlines=True,
                cwd=script_dir,
                timeout=EXPORT_TIMEOUT
            )
            
            if result.returncode == 0:
                log("[{0}/{1}] TrustID {2} 导出成功".format(index, total, trust_id), log_file)
                success_count += 1
            else:
                log("[{0}/{1}] TrustID {2} 导出失败".format(index, total, trust_id), log_file)
                stderr = result.stderr.strip() if result.stderr else "无错误信息"
                log("  错误信息: {0}".format(stderr), log_file)
                failed_ids.append(trust_id)
        
        except subprocess.TimeoutExpired:
            log("[{0}/{1}] TrustID {2} 导出超时 (超过{3}秒)".format(
                index, total, trust_id, EXPORT_TIMEOUT), log_file)
            failed_ids.append(trust_id)
                
        except Exception as e:
            log("[{0}/{1}] TrustID {2} 导出异常: {3}".format(
                index, total, trust_id, str(e)), log_file)
            failed_ids.append(trust_id)
    
    # 输出汇总
    log("=" * 50, log_file)
    log("导出完成! 成功: {0}/{1}".format(success_count, total), log_file)
    
    if failed_ids:
        log("失败的 TrustID: {0}".format(', '.join(failed_ids)), log_file)
        # 将失败的 ID 写入文件，方便重试
        base_name = os.path.splitext(txt_file)[0]
        failed_file = base_name + "_failed.txt"
        with open(failed_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(failed_ids))
        log("失败的 TrustID 已保存到: {0}".format(failed_file), log_file)
    
    log("详细日志已保存到: {0}".format(log_file), log_file)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python batch_export.py <trustids.txt>")
        print("  trustids.txt: 包含 TrustID 的文本文件，每行一个 ID")
        print("")
        print("示例文件格式:")
        print("  # 这是注释，会被跳过")
        print("  26065")
        print("  26066")
        sys.exit(1)
    
    txt_file = sys.argv[1]
    batch_export(txt_file)
