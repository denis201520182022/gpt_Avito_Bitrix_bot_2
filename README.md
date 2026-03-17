systemctl start Avito_Lesan_bot_2.service
systemctl status Avito_Lesan_bot_2.service
systemctl stop Avito_Lesan_bot_2.service
systemctl restart Avito_Lesan_bot_2.service

journalctl -u Avito_Lesan_bot_2.service -f