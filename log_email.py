import os
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def log_email(fromaddr, toaddr, password, subject, body):
    """Function send an email based on the information provided

    :param fromaddr: Sender email id
    :type fromaddr: string
    :param toaddr: Receiver email id
    :type toaddr: string
    :param password: Password for the sender email id
    :type password: string
    :param subject: Email Subject
    :type subject: string
    :param body: Email Body
    :type body: string
    """

    # instance of MIMEMultipart
    msg = MIMEMultipart()
    # storing the senders email address
    msg['From'] = fromaddr
    # storing the receivers email address
    msg['To'] = toaddr
    # storing the subject
    msg['Subject'] = subject

    # attach the body with the msg instance
    msg.attach(MIMEText(body, 'plain'))
    # Check if errlog file is available and readable
    time.sleep(5)
    try:
        # creates SMTP session
        s = smtplib.SMTP('smtp.gmail.com', 587)
        # start TLS for security
        s.starttls()
        # Authentication
        s.login(fromaddr, password)
        # Converts the Multipart msg into a string
        text = msg.as_string()
        # sending the mail
        s.sendmail(fromaddr, toaddr.split(','), text)
        print("[MESSAGE] Email sent.", "\n")
    except IOError:
        print("[WARNING] something went wrong while sending email.", "\n")
