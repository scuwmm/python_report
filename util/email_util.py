import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

smtp_server = 'smtp.exmail.qq.com'
smtp_port = 587
smtp_username = 'data@youxiang.io'
smtp_password = 'P@ssvv0rd'
smtp_postfix = 'youxiang.io'


def send_attachment_email(subject, body, to, file_path, file_name):
    # 创建 MIMEMultipart 对象
    msg = MIMEMultipart()
    msg['From'] = smtp_username
    msg['To'] = to
    msg['Subject'] = subject

    # 添加邮件正文
    msg.attach(MIMEText(body, 'plain'))

    # 附加附件
    attachment = open(file_path, 'rb')

    part = MIMEBase('application', 'octet-stream')
    part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename= {file_name}')
    msg.attach(part)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(smtp_username, to, msg.as_string())

