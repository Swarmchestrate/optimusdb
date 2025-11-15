# remove any previous Go in /usr/local
sudo rm -rf /usr/local/go

# install Go 1.19.13 system-wide
wget https://go.dev/dl/go1.19.13.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.19.13.linux-amd64.tar.gz

# put it on PATH for everyone
sudo ln -sf /usr/local/go/bin/go /usr/bin/go
sudo ln -sf /usr/local/go/bin/gofmt /usr/bin/gofmt

# verify (both should work)
go version
sudo go version
