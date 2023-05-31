import time
import datetime

from util import db_util
from util import email_util
import util.csv_util as csv_util

group_keys = '"bidaobi","liangjianshequ","HaXiSQ","xiaomutu","touhaowanjia8","xiaochizi12345678","LSMM8","JCJ9527",' \
             '"WangPaiXiaoDui","AncientPuzzledBoy","gongfushequ","HualiuNB","wenwenhouhuayuan","DINGDANGSQ",' \
             '"CryptoTribal","FAcairjshequ","buybuybuybi","xiaoleyuan","CFSQ6","bacommu","moonbays","TangTangZonghe",' \
             '"wajinshequ","LYST88","babylisacaiyun","Hunter1888","wennuanshequ","lantianshequ",' \
             '"BraisedChickenCommunity","dongfangshenting8","zhuzhudui","quanzhangshequ","gemhaiwai","liangxinshequ",' \
             '"XiaoguiSQ","DGreatwhale2","sannianjiyiban","datoutoudexiaowu","insidejwr","GOUHOO",' \
             '"ChinaWhaleCommunity","xianyuCapitalCN","ggulu","Xiake_crypto_community","ShadowAssassin567",' \
             '"Weidushequchina","mumarensq","MysteryDaily1","cxsqcb","longzuqkl","windstormSQ","BiYouSQ","jingouss",' \
             '"BIYINGSQ","tiedaoyoujidui" '

to_email = "aaron@youxiang.io,java@youxiang.io"
to_email = "java@youxiang.io,hack@youxiang.io,seb@youxiang.io"


def did_report():
    try:
        # Step1：查询社区DID数据
        connection = db_util.get_btok_connection()
        cursor = connection.cursor()
        sql = """SELECT t2.group_key '群key',t1.did_id '钱包ID',t1.name 'DID佩戴币种',t1.did_time 'DID开通时间',t3.create_time '注册时间'  FROM 
            (SELECT d.telegram_id,d.user_id,d.did_id,i.`name`,d.create_time did_time  FROM biyong_main.did_wallet d JOIN biyong_main.did_info i ON d.did_id=i.id WHERE d.enable_wear=1) t1
            JOIN 
            (
                SELECT telegram_id,group_key FROM biyong_main.user_group_relevance WHERE `status`=0 AND group_key IN (
                %s
                )
                UNION
                SELECT telegram_id,group_key FROM biyong_main.un_user_group_relevance WHERE `status`=0 AND group_key IN (
                %s
                )
            ) t2 ON t1.telegram_id=t2.telegram_id
            JOIN biyong_main.user_biyong t3 ON t1.user_id=t3.id
            ORDER BY 1
            LIMIT 100000
            ;""" % (group_keys, group_keys)

        cursor.execute(sql)
        result = cursor.fetchall()

        print("data size=", len(result))

        # Step2 生成csv文件
        TD = datetime.date.today()
        csv_dir = csv_util.get_csv_path("did")
        file_name = "DidGroupData_%s_%d.csv" % (TD, time.time())
        file_path = csv_dir + "/" + file_name
        column_names = [desc[0] for desc in cursor.description]
        csv_util.to_csv_file(file_path, column_names, result)

        # Step3 发送email
        subject = "社区DID数据_%s" % TD
        body = "数据见附件"
        email_util.send_attachment_email(subject, body, to_email, file_path, file_name)

        print("did report success!")

    except Exception as e:
        print("get_today_swap_data error:", e)
    finally:
        connection.close()
        cursor.close()


if __name__ == '__main__':
    did_report()
