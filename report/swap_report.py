import time

import os
import util.db_util as db_util
import util.csv_util as csv_util
import util.email_util as email_util
import datetime

to_email = "aaron@youxiang.io"


# to_email = "aaron@youxiang.io,java@youxiang.io,hack@youxiang.io"


# 将node库中的数据导入到btok库中（两张table结构完全一致）
def swap_report():
    try:
        # Step1:在btok数据获取最大id
        btok_connection = db_util.get_btok_connection()
        btok_cursor = btok_connection.cursor()

        btok_cursor.execute("select max(id) FROM biyong_main.transit_swap_record;")
        result = btok_cursor.fetchall()
        max_id = result[0]

        # Step2:在node查询最新的swap数据
        node_connection = db_util.get_node_connection()
        node_cursor = node_connection.cursor()
        sql = "SELECT * FROM node.transit_swap_record WHERE id > %d ORDER BY id DESC" % max_id
        node_cursor.execute(sql)
        result = node_cursor.fetchall()

        # Step3:将swap数据导入到btok库（）
        column_names = [desc[0] for desc in node_cursor.description]
        insert_query = "INSERT INTO biyong_main.transit_swap_record VALUES ({})".format(
            ','.join(['%s'] * len(column_names)))
        btok_cursor.executemany(insert_query, result)
        btok_connection.commit()

        # Step4:查询btok数据
        result_sql = """SELECT 
                tsr.id,
                tsr.tx_id '交易hash',tsr.trader '去中心化钱包地址',
                case when tsr.src_token='0x0000000000000000000000000000000000000000' THEN 'BNB' else ec_src.coin_name end '发送方交易币种名称',tsr.src_token '发送发交易币种',tsr.amount/pow(10,coalesce(ec_src.token_decimal,0)) '发送方交易数量',
                case when tsr.dst_token='0x0000000000000000000000000000000000000000' THEN 'BNB' else ec_dst.coin_name end '接收方交易币种名称',tsr.dst_token '接收方交易币种',tsr.return_amount/pow(10,coalesce(ec_dst.token_decimal,18)) '接收方交易数量',
                tsr.`time` '交易时间', tsr.fee_token '手续费币种',
                case when tsr.fee_token='0x0000000000000000000000000000000000000000' then 'BNB' when tsr.src_token=tsr.fee_token then ec_src.coin_name else ec_dst.coin_name end '手续费币种名称'
                ,case when tsr.src_token=tsr.fee_token then tsr.fee/pow(10,coalesce(ec_src.token_decimal,0)) else tsr.fee/pow(10,coalesce(ec_dst.token_decimal,0)) end '手续费数量'
                ,ub.phone '用户手机号',ua.id '用户中心化钱包ID',ub.id '用户ID'
                ,tg.group_key '群Key',tg.group_name '群组名称',tg.group_id '群组TGID'
                ,ec_src.token_decimal 'src_decimal',ec_dst.token_decimal 'dst_decimal'
                FROM biyong_main.transit_swap_record tsr
                LEFT JOIN biyong_main.user_third_address uta ON tsr.trader=uta.address
                LEFT JOIN biyong_main.user_biyong ub ON uta.user_id=ub.id
                LEFT JOIN biyong_main.user_address ua on ua.user_id=ub.id
                LEFT JOIN biyong_main.telegram_group tg on tsr.`source`= tg.group_id
                LEFT JOIN biyong_main.erc20_contract ec_src on ec_src.contract_id=tsr.src_token
                LEFT JOIN biyong_main.erc20_contract ec_dst on ec_dst.contract_id=tsr.dst_token
                WHERE tsr.id > %d
                order by tsr.id desc
                ;""" % max_id
        btok_cursor.execute(result_sql)
        result = btok_cursor.fetchall()

        print("data size=", len(result))

        # Step5: 将数据导成csv文件
        # 获取当前执行文件的绝对路径
        TD = datetime.date.today()
        csv_dir = csv_util.get_csv_path("swap")
        file_name = "SwapData_%s_%d.csv" % (TD, time.time())
        file_path = csv_dir + "/" + file_name
        column_names = [desc[0] for desc in btok_cursor.description]
        csv_util.to_csv_file(file_path, column_names, result)

        # Step6: 发送email
        subject = "Swap交易数据_%s" % TD
        body = "数据见附件"
        email_util.send_attachment_email(subject, body, to_email, file_path, file_name)

        print("swap report success.")

    except Exception as e:
        print("get_today_swap_data error:", e)
    finally:
        btok_connection.close()
        btok_cursor.close()
        node_connection.close()
        node_cursor.close()


# 检查是否作为主程序直接执行
if __name__ == "__main__":
    # 调用主函数
    swap_report()
