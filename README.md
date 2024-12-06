# MP 4 - Group 39

To build and run locally
```
go build
./mp4
```

To build and run on VMs
```
env GOOS=linux GOARCH=amd64 go build
scp ./mp4 user@server:/...
# Run ./mp4 on server
```