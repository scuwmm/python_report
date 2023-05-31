import csv


def to_csv_file(file, column_names, result):
    with open(file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # 写入列名
        writer.writerow(column_names)

        # 写入数据
        writer.writerows(result)
