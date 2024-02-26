# 2.1 Fetch a Web page

```
Trying 104.196.238.229...
Connected to cs144.keithw.org.
Escape character is '^]'.
GET /lab0/sunetid HTTP/1.1
Host: cs144.keithw.org
Connection: close

HTTP/1.1 200 OK
Date: Mon, 26 Feb 2024 06:18:38 GMT
Server: Apache
X-You-Said-Your-SunetID-Was: sunetid
X-Your-Code-Is: 983208
Content-length: 111
Vary: Accept-Encoding
Connection: close
Content-Type: text/plain

Hello! You told us that your SUNet ID was "sunetid". Please see the HTTP headers (above) for your secret code.
Connection closed by foreign host.
```

# 2.2 Send yourself an email

**QQ 邮箱版本**

587 端口是 smtp 加密连接的默认端口 <br>
原 Lab 中是非加密的 25 端口

```
> telnet smtp.qq.com 587
Trying 183.47.101.192...
Connected to smtp.qq.com.
Escape character is '^]'.
220 newxmesmtplogicsvrszc5-2.qq.com XMail Esmtp QQ Mail Server.
Helo qq.com
250-newxmesmtplogicsvrszc5-2.qq.com-30.174.48.222-51335865
250-SIZE 73400320
250 OK
auth login
334 VXNlcm5hbWU6
输入用户名的 Base64 版本
334 UGFzc3dvcmQ6
输入授权码的 Base64 版本
235 Authentication successful
MAIL FROM: <xxxxxxx@qq.com>
250 OK
RCPT TO: <xxxxxxx@qq.com>
250 OK
DATA
354 End data with <CR><LF>.<CR><LF>.
From: xxxxxxx@qq.com
To: xxxxxxx@qq.com
Subject: Hello from CS144 Lab 0!

.
250 OK: queued as.
QUIT
221 Bye.
Connection closed by foreign host.
```

成功收到邮件

# 2.3 Listening and connecting

```
> netcat -v -l -p 9090
Listening on LAPTOP-RPT2HO0D 9090
Connection received on localhost 55844
Hello
Nice Tool!
^C
```

```
> telnet localhost 9090
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
Hello
Nice Tool!
Connection closed by foreign host.
```