podman build --platform linux/amd64,linux/arm64  --manifest 758373708967.dkr.ecr.eu-central-1.amazonaws.com/tfmigrate:latest  .
podman manifest push 758373708967.dkr.ecr.eu-central-1.amazonaws.com/tfmigrate:latest

