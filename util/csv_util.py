import csv
import os


# 获取 /python_report/csv/{child_dir} 目录
def get_csv_path(child_dir):
    # 当前执行文件目录 /python_report/util/csv_util.csv
    current_file = os.path.abspath(__file__)
    # 获取当前执行文件的相对目录  /python_report/util
    current_dir = os.path.dirname(current_file)
    # 获取当前执行文件所在的父级文件夹路径 /python_report
    parent_dir = os.path.dirname(current_dir)
    # 构建目标相对目录路径 /python_report/csv/{child_dir}
    csv_dir = os.path.join(parent_dir, "csv", child_dir)
    return csv_dir


# 将数据生成csv文件
def to_csv_file(file, column_names, result):
    with open(file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # 写入列名
        writer.writerow(column_names)

        # 写入数据
        writer.writerows(result)
