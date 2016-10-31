CREATE EXTERNAL TABLE sender_receivers_rcfile (sender STRING, receiver1 STRING,
receiver2 STRING, receiver3 STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS RCFILE;

INSERT OVERWRITE TABLE sender_receivers_rcfile SELECT * FROM sender_receivers;

SELECT x.sender, 
(case when y1.receiver1 = x.sender then null else y1.receiver1 end),
(case when y1.receiver2 = x.sender then null else y1.receiver2 end),
(case when y1.receiver3 = x.sender then null else y1.receiver3 end),
(case when y2.receiver1 = x.sender then null else y2.receiver1 end),
(case when y2.receiver2 = x.sender then null else y2.receiver2 end),
(case when y2.receiver3 = x.sender then null else y2.receiver3 end),
(case when y3.receiver1 = x.sender then null else y3.receiver1 end),
(case when y3.receiver2 = x.sender then null else y3.receiver2 end),
(case when y3.receiver3 = x.sender then null else y3.receiver3 end)
FROM sender_receivers_rcfile x 
LEFT JOIN sender_receivers_rcfile y1 ON (x.receiver1 = y1.sender)
LEFT JOIN sender_receivers_rcfile y2 ON (x.receiver2 = y2.sender)
LEFT JOIN sender_receivers_rcfile y3 ON (x.receiver3 = y3.sender)
WHERE (y1.receiver1 IS NOT NULL OR y2.receiver1 IS NOT NULL OR y3.receiver1 IS NOT NULL)
ORDER BY x.sender;
