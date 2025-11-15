
#!/bin/bash

################################################################################
# uninstall_EMS.sh
#
# Description:
# Uninstalls the EMS server deployment from Kubernetes (Helm release + namespace
# resources) and optionally removes local Docker images.
#
# Author: George Georgakakos, ICCS
# Updated: 2025-09-13
################################################################################


cd /opt/iccs/manifests/
sudo kubectl delete -f optimusdb-k3s.yaml
sudo kubectl delete pvc data-optimusdb-0 -n default
sudo kubectl delete pvc data-optimusdb-1 -n default
sudo kubectl delete pvc data-optimusdb-2 -n default

sudo kubectl get pods -A | grep optimusdb
sudo kubectl get svc -A | grep optimusdb

sudo kubectl apply -f optimusdb-k3s.yaml
