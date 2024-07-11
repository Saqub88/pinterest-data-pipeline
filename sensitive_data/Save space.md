AWS IAM Username: 0aa58e5ad07d

SSH Keypair ID: 

key-0c2cac2a82e619972

-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAidYD4bDZGSf6XgfMEKeOfYyo75LNf0kKsRTIW9y/M0/0w24D
xy5WVo7fXeoGwiCIgcbu8qdySizJ3AtRHjTWUCRFASaVCVKGaYGgPohaX9Cn1n72
Q4YngCKY6KohFH40Whj9UBaXbBXA5D0oS9q2VZsR6WHIU6cRDXP0RyCX+KGHrpDI
7GW3ZWw68k48W6j+5EuX1mjAdl+jYNUpZx/q84uh3OumfFAUt3VQQNd5Ni5E/hJR
/245z4WYlJumkIfr361Lv1WenNGW7XbYPwV+wKeVg+JvC/LtPCKaxp0CGwvsGPvO
TVcyRRLidaO3Nu4leSDZyPyNAUoBezMT+uZXdwIDAQABAoIBAHnwJXryMv14SpwJ
FdTeBsa3O3TGUJn8ikLieeYaweOpJoYFu73oFmV4+85fjR8MHPeCgD2XORq5yNKs
CFVKtRYtIAbGlS6sHVEVZigsBJejRfgSVdaixisrcE7Uw3MXu0TBusexVMA/9md0
W0PP92KATzZCVTD8Ka/XFxIdmZRQNdj6BSkIxoAMP0hiZFNzoIN3M0VycWVwlQr3
89a2TfIPmbF4Qz5nP0GD7ucOQuI84QqDYySLYpdhsqctzwNcMaVCGzesTsxSbaIb
lWhHHzpPf9syJYqOKkEDyX+IUaTtKz3xT1eIpniiVCBhsFKeP27SdJNWOvKtDbsE
XnHrmXECgYEAwwz8Mjl2orqOApKUjweGKHjfBNR1N6JGpLh4alolNxX8L7US1pDR
KZeg0cUE3eAlRlUsR24dxWoAH+obiPdsg2C3ybE7stHqJOOo/VuQaMlHYmzgzdFi
POg/JjxrY+etPbnA8bmg9D4P8SRod2Zo2sfwEYaw7ZLgbjjLisRkTnMCgYEAtOgo
aLqyU3FknMLfJMJI2JiWN7psB2Tc/siFKXOjZ+iyugIczqd1x6ot1/jWTGKjT0n0
j5RZi6dj2hE/EOunQB1OwFl3cEfEolUS2nBROb4xN9+HyT9aVK8RVqiwFuV6Nxr0
nVyHA7GBwGQKynM14j5zYQa35crNOMJ4YQNire0CgYBZfn3Ataj/ZUV/XK4ZKkSe
EUeSucDGtAhhz9pAQfhNXCMH6LcqB+xQfyfk19cxzoSLzsywQZAbYlWITywvHgeG
CDyLqGxR0gryvhptlTOfQKsmN/q1tNq0Q4OmEZbckhJk/fPPdXKqXkeGR0Q/dJer
uo0mHXzUpM2hBSIvG4lCjQKBgBTGTE2qkuvuK6Ws4z8vaHonG6kOqXU36gEAOfqG
ow89s7iUoYZByM7DBjy1ALcI5MQoNAOA/79/24GhVWK2DSDZvL9uBr9IHpadumqH
V2yQIMrPyqIbul3bNNyExqP6ekx1tf2UMJUF/2Z+lpalIFz42vsbcGbdITARdQ1+
/AgJAoGBAJlqu8LwzkPOGIfKtt7dwWyqZi4tAJIHEFl8H6Ojdkr29wDL1yw2cCkU
gRQdkwxugo5G4Tuy3Gqlto4APNqY961P76DNOZKomViCwPINHK8bvGbeeT3cSq79
sbyTpE8EUSXSOOdsKba6JJcDaKqzxkYUIwaDw7Gjfm9NlJSpfpoK
-----END RSA PRIVATE KEY-----

key pair name : 0aa58e5ad07d-key-pair

command to connect to ec2 console
ssh -i "Sensitive data/0aa58e5ad07d-key-pair.pem" ec2-user@ec2-54-242-40-15.compute-1.amazonaws.com

0aa58e5ad07d-ec2-access-role
arn:aws:iam::584739742957:role/0aa58e5ad07d-ec2-access-role


Bootstrap server string
b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098

plaintext apache zookeeper connection string
z-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:2181,z-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:2181,z-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:2181

S3 bucket
user-0aa58e5ad07d-bucket

Invoke URL
https://m1c8pv5ln1.execute-api.us-east-1.amazonaws.com/test

to start rest proxy. navigate to confluent-7.2.0/bin. run the following command :
./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties