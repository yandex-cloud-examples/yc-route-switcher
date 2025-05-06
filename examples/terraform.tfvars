// folder id for resources
folder_id = "b1genmlel3ax7ux47jko"

// VPC name for resources
vpc_name = "demo"

// list of trusted public IP addresses for connection to NAT-instances 
trusted_ip_for_mgmt = ["A.A.A.A/32", "B.B.B.0/24"]

// username for VMs
# Here you must specify the username that will be granted root privileges for working in the serial console. 
# Be careful — you must not use "root" or "admin" as the username, as this will cause validation errors and prevent proper execution of cloud-init.
vm_username = "devops"

// private subnets
private_subnet_a_name = "private-a"
private_subnet_a_cidr = "10.160.1.0/24" 

// public subnets
public_subnet_a_name = "public-a"
public_subnet_b_name = "public-b"
public_subnet_a_cidr = "172.16.1.0/24" 
public_subnet_b_cidr = "172.16.2.0/24" 
