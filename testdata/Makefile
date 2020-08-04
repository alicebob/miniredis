gen:
	openssl req \
    	-x509 \
    	-nodes \
    	-newkey rsa:2048 \
    	-keyout server.key \
    	-out server.crt \
    	-days 3650 \
    	-addext "subjectAltName = DNS:Server" \
    	-subj "/C=GB/ST=London/L=London/O=Global Security/OU=IT Department"
	openssl req \
    	-x509 \
    	-nodes \
    	-newkey rsa:2048 \
    	-keyout client.key \
    	-out client.crt \
    	-days 3650 \
    	-addext "subjectAltName = DNS:Client" \
    	-subj "/C=GB/ST=London/L=London/O=Global Security/OU=IT Department"
