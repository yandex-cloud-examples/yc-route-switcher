import yaml
import os
import boto3
import requests
import concurrent.futures as pool
import time
import datetime

iam_token = ""
endpoint_url='https://storage.yandexcloud.net'
path = os.getenv('CONFIG_PATH')
bucket = os.getenv('BUCKET_NAME')
cron_interval = int(os.getenv('CRON_INTERVAL'))
# validate if cron_interval more than 10 minutes, than limit it to 10
if cron_interval > 10:
    cron_interval = 10
back_to_primary = os.getenv('BACK_TO_PRIMARY').lower()
router_healthcheck_interval = int(os.getenv('ROUTER_HCHK_INTERVAL'))
# validate if router_healthcheck_interval less than 10 seconds, than increase it to 10 seconds
if router_healthcheck_interval < 10:
    router_healthcheck_interval = 10
folder_name = os.getenv('FOLDER_NAME')
function_name = os.getenv('FUNCTION_NAME')

def get_config(endpoint_url='https://storage.yandexcloud.net'):
    '''
    gets config in special format from bucket
    :param endpoint_url: url of object storage
    :return: configuration dictionary from bucket with route tables and load balancer id and list with route table ids and its actual routes in VPC 
    '''

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url=endpoint_url
    )

    try:    
        response = s3_client.get_object(Bucket=bucket, Key=path)
        config = yaml.load(response["Body"], Loader=yaml.FullLoader)
    except Exception as e:
        print(f"Request to get configuration file {path} in bucket failed due to: {e}. Please check that the configuration file exists in bucket {bucket}. Retrying in {cron_interval} minutes...")
        return
    
    return config

def get_router_status(config):
    '''
    get routers status from NLB 
    :param config: configuration dictionary with route tables and load balancer id
    :return: dictionary (targetStatus) with healthchecked IP address of routers and its state
    '''

    targetStatus = {}
    metrics = list()

    # get router status from NLB
    try:    
        r = requests.get("https://load-balancer.api.cloud.yandex.net/load-balancer/v1/networkLoadBalancers/%s:getTargetStates?targetGroupId=%s" % (config['loadBalancerId'], config['targetGroupId']), headers={'Authorization': 'Bearer %s'  % iam_token})
    except Exception as e:
        print(f"Request to get target states in load balancer {config['loadBalancerId']} failed due to: {e}. Retrying in {cron_interval} minutes...")
        return 
    
    if r.status_code != 200:
        print(f"Unexpected status code {r.status_code} for getting target states in load balancer {config['loadBalancerId']}. More details: {r.json().get('message')}. Retrying in {cron_interval} minutes...")
        return 

    if 'targetStates' in r.json():
        if len(r.json()['targetStates']) < 2:
            # check whether we have at least two routers configured, if not return and generate an error
            print(f"At least two routers should be in load balancer {config['loadBalancerId']}. Please add one more router. Retrying in {cron_interval} minute...")
            return 
        else:
            # prepare targetStatus dictionary (targetStatus) with {key:value}, where key - healthchecked IP address of router, value - HEALTHY or other state
            for target in r.json()['targetStates']:
                targetStatus[target['address']] = target['status']
            if 'HEALTHY' not in targetStatus.values():
                # all routers are not healthy, exit from function 
                print(f"All routers are not healthy. Can not switch next hops for route tables. Retrying in {cron_interval} minutes...")
                for target in r.json()['targetStates']:
                    # add custom metric 'route_switcher.router_state' into metric list for Yandex Monitoring that router state is not healthy
                    metrics.append({"name": "route_switcher.router_state", "labels": {"router_ip": target['address'], "folder_name": folder_name}, "type": "IGAUGE", "value": 0, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})
                # write metrics into Yandex Monitoring
                write_metrics(metrics)
                return
            return targetStatus 
    else:
        print(f"There are no target endpoints in load balancer {config['loadBalancerId']}. Please add two endpoints. Retrying in {cron_interval} minutes...")
        return    


def get_config_route_tables_and_routers():
    '''
    get config in special format from bucket
    get actual routes from route tables in VPC which are protected by route-switcher 
    :return: configuration dictionary from bucket with route tables and load balancer id and list with route table ids and its actual routes in VPC 
    '''

    # get config from bucket 
    config = get_config()
    if config is None:
        return

    # check whether we have routers in config
    if 'routers' in config:
        if config['routers'] is None:        
            print(f"Routers configuration does not exist. Please add 'routers' input variable for Terraform route-switcher module. Retrying in {cron_interval} minute...")
            return
    else:
        print(f"Routers configuration does not exist. Please add 'routers' input variable for Terraform route-switcher module. Retrying in {cron_interval} minutes...")
        return
    
    # check whether we have route tables in config
    if 'route_tables' in config:
        if config['route_tables'] is None:
            # check whether we have at least one route table in config
            print(f"There are no route tables in config file in bucket. Please add at least one route table. Retrying in {cron_interval} minutes...")
            return
    else:
        print(f"There are no route tables in config file in bucket. Please add at least one route table. Retrying in {cron_interval} minutes...")
        return
    
    # get routers status from NLB 
    routerStatus = get_router_status(config)
    if routerStatus is None:
        # exit from function as some errors happened when checking router status
        return
    
    nexthops = {}
    router_error = False
    security_groups = False
    vm_id_counter = 0
    primary_counter = 0
    routers = {}
    for router in config['routers']:
        if 'healthchecked_ip' in router and router['healthchecked_ip']:
            router_hc_address = router['healthchecked_ip']
        else:
            print(f"Router does not have 'healthchecked_ip' configuration. Please add 'healthchecked_ip' value in 'routers' input variable for Terraform route-switcher module.")
            router_error = True
            continue
        if router_hc_address in routerStatus:
            if 'interfaces' in router and router['interfaces']:
                router_interfaces = router['interfaces']
            else:
                print(f"Router {router_hc_address} does not have 'interfaces' configuration. Please add 'interfaces' list in 'routers' input variable for Terraform route-switcher module.")
                router_error = True
                continue
            
            for interface in router_interfaces:
                if ('index' in interface and interface['index'] is not None) or ('security_group_ids' in interface and interface['security_group_ids']):
                    if ('index' in interface and interface['index'] is not None) and ('security_group_ids' in interface and interface['security_group_ids']):
                        if 'vm_id' not in router or not router['vm_id']:
                            print(f"Router {router_hc_address} does not have 'vm_id' configuration and has 'index' and 'security groups' configuration for interfaces. Please add 'vm_id' value in 'routers' input variable for Terraform route-switcher module. Retrying in {cron_interval} minutes...")
                            router_error = True
                        else:
                            security_groups = True
                    else:
                        if 'index' in interface and interface['index'] is not None:
                            print(f"Router {router_hc_address} does not have 'security groups' configuration for interface with {interface['index']} index. Please add 'security_group_ids' value in 'interfaces' input variable for Terraform route-switcher module.")
                            router_error = True
                        if 'security_group_ids' in interface and interface['security_group_ids']:
                            print(f"Router {router_hc_address} does not have 'index' configuration for interface with {interface['security_group_ids']} security groups. Please add 'index' value in 'interfaces' input variable for Terraform route-switcher module.")
                            router_error = True
                # prepare dictionary with router nexthops as {key:value}, where key - nexthop address, value - nexthop address of backup router
                if ('own_ip' in interface and interface['own_ip']) or ('backup_peer_ip' in interface and interface['backup_peer_ip']):
                    if ('own_ip' in interface and interface['own_ip']) and ('backup_peer_ip' in interface and interface['backup_peer_ip']):
                        nexthops[interface['own_ip']] = interface['backup_peer_ip']
                        # prepare dictionary with router healthcheck IP addresses as {key:value}, where key - nexthop address, value - router healthcheck IP address of this nexthop address
                        routers[interface['own_ip']] = router_hc_address      
                    else:
                        if 'backup_peer_ip' in interface and interface['backup_peer_ip']:
                            print(f"Router {router_hc_address} does not have 'own_ip' configuration for interface. Please add 'own_ip' value in 'interfaces' input variable for Terraform route-switcher module.")
                            router_error = True
                        if 'own_ip' in interface and interface['own_ip']:
                            print(f"Router {router_hc_address} does not have 'backup_peer_ip' configuration for interface. Please add 'backup_peer_ip' value in 'interfaces' input variable for Terraform route-switcher module.")
                            router_error = True                    
            if 'vm_id' in router and router['vm_id']:
                vm_id_counter += 1
                if not security_groups:
                    print(f"Router {router_hc_address} has 'vm_id' configuration and does not have 'index' and 'security groups' configuration for interfaces. Please add 'index' and 'security_group_ids' value in 'interfaces' input variable for Terraform route-switcher module or remove 'vm_id' value for router {router_hc_address} configuration.")
                    router_error = True
                if 'primary' in router and router['primary']:
                    primary_counter += 1
        else:
            print(f"Router {router_hc_address} is not in target endpoints of load balancer {config['loadBalancerId']}. Please check load balancer configuration or 'routers' input variable for Terraform route-switcher module.")
            router_error = True

    if vm_id_counter:
        if primary_counter != 1:
            print(f"There should be one router with 'primary = true' configuration. Please add 'primary = true' value to only one router with 'vm_id' value in 'routers' input variable for Terraform route-switcher module. Retrying in {cron_interval} minutes...")
            router_error = True
        if vm_id_counter != 2:
            print(f"There should be two routers with 'vm_id' configuration. Please add 'vm_id' value to only two routers in 'routers' input variable for Terraform route-switcher module. Retrying in {cron_interval} minutes...")
            router_error = True

    all_routeTables = {}
    config_changed = False
    route_table_error = False
    for config_route_table in config['route_tables']:
        try:    
            r = requests.get("https://vpc.api.cloud.yandex.net/vpc/v1/routeTables/%s" % config_route_table['route_table_id'], headers={'Authorization': 'Bearer %s'  % iam_token})
        except Exception as e:
            print(f"Request to get route table {config_route_table['route_table_id']} failed due to: {e}. Retrying in {cron_interval} minutes...")
            route_table_error = True
            continue
        
        if r.status_code != 200:
            print(f"Unexpected status code {r.status_code} for getting route table {config_route_table['route_table_id']}. More details: {r.json().get('message')}. Retrying in {cron_interval} minutes...")
            route_table_error = True
            continue

        if 'staticRoutes' in r.json():
            routeTable = r.json()['staticRoutes']
            if not len(routeTable):
                # check whether we have at least one route configured
                print(f"There are no routes in route table {config_route_table['route_table_id']}. Please add at least one route.")
                route_table_error = True
                continue

            routeTable_prefixes = set()
            for ip_route in routeTable: 
                # checking if next hop is one of a router addresses
                if 'nextHopAddress' in ip_route:
                    if ip_route['nextHopAddress'] in nexthops:
                        # populate routeTable_prefixes set with route table prefixes
                        routeTable_prefixes.add(ip_route['destinationPrefix'])
                    
                        if 'routes' in config_route_table:
                            if ip_route['destinationPrefix'] not in config_route_table['routes']:
                                # insert route in config file stored in bucket
                                config_route_table['routes'].update({ip_route['destinationPrefix']:ip_route['nextHopAddress']}) 
                                config_changed = True
                        else:
                            # insert route in config file stored in bucket
                            config_route_table['routes'] = {}
                            config_route_table['routes'].update({ip_route['destinationPrefix']:ip_route['nextHopAddress']}) 
                            config_changed = True
                
            if 'routes' in config_route_table:
                if len(set(config_route_table['routes'].keys())) != len(routeTable_prefixes):
                    # if there are some routes left in config file but deleted from actual route table
                    for prefix in set(config_route_table['routes'].keys()).difference(routeTable_prefixes):
                        # delete route from config file as it does not exist in actual route table
                        config_route_table['routes'].pop(prefix)
                    config_changed = True

            # add route table to all_routeTables dictionary
            all_routeTables.update({config_route_table['route_table_id']:{'name':r.json()['name'],'staticRoutes':sorted(routeTable, key=lambda i: i['destinationPrefix'])}})
        else:
            print(f"There are no routes in route table {config_route_table['route_table_id']}. Please add at least one route.")
            route_table_error = True
            continue

    if config_changed:
        # if routes were inserted or deleted from config file need to update it in bucket 
        print(f"Store updated route tables config file in bucket: {config['route_tables']}")
        put_config(config)

    error_message = None
    if router_error:
        error_message = f"Some routers have errors in configuration file in bucket (see more details in log). Waiting for correct routers configuration. Retrying in {cron_interval} minutes..."
    elif route_table_error:
        error_message = f"Some route tables have errors in configuration file in bucket or during VPC API request (see more details in log). Waiting for correct route tables configuration. Retrying in {cron_interval} minutes..."

    return {'config':config, 'all_routeTables':all_routeTables, 'routers':routers, 'error_message':error_message}


def get_diff_security_groups(vm_id, healthchecked_ip, config_router_interfaces):
    '''
    get difference between current list of security group ids for a router and list of security group ids in configuration file from bucket to compare with 
    :return: list of router network interfaces with list of security groups which should be applied
    '''

    network_interfaces_security_group_ids = {}
    all_modified_router_network_interfaces = list()
    # get security groups for network interfaces from Compute API
    try:  
        r = requests.get("https://compute.api.cloud.yandex.net/compute/v1/instances/%s" % vm_id, headers={'Authorization': 'Bearer %s'  % iam_token})
    except Exception as e:
        print(f"Request to get security groups for router {healthchecked_ip} network interfaces failed due to: {e}. Retrying in {cron_interval} minutes...")
        return     
    if r.status_code != 200:
        print(f"Unexpected status code {r.status_code} for getting security groups for router {healthchecked_ip} network interfaces. More details: {r.json().get('message')}. Retrying in {cron_interval} minutes...")
        return 
    if 'networkInterfaces' in r.json():
        for interface in r.json()['networkInterfaces']:
            # prepare dictionary with router network interfaces with security groups as {key:value}, where key - router interface index, value - current list of security group ids
            network_interfaces_security_group_ids[interface['index']] = interface['securityGroupIds']
    else:
        print(f"There are no network interfaces in router {healthchecked_ip}. Please add required network interfaces. Retrying in {cron_interval} minutes...")
        return
    
    for config_interface in config_router_interfaces:
        if config_interface['index'] is not None:
            # if there is difference between current security groups for routers and list of security groups in configuration file which we need to compare 
            if (sorted(network_interfaces_security_group_ids[str(config_interface['index'])]) != sorted(config_interface['security_group_ids'])):
                if 'last_operation_id' in config_interface and config_interface['last_operation_id']:
                    all_modified_router_network_interfaces.append({'router_hc_address': healthchecked_ip, 'vm_id': vm_id, 'index': config_interface['index'], 'security_group_ids': config_interface['security_group_ids'], 'last_operation_id': config_interface['last_operation_id']})
                else:
                    all_modified_router_network_interfaces.append({'router_hc_address': healthchecked_ip, 'vm_id': vm_id, 'index': config_interface['index'], 'security_group_ids': config_interface['security_group_ids'], 'last_operation_id': None})
    
    if all_modified_router_network_interfaces:
        return all_modified_router_network_interfaces           

    
def put_config(config, endpoint_url='https://storage.yandexcloud.net'):
    '''
    uploads config file to the bucket
    :param config: configuration dictionary with route tables and load balancer id
    :param endpoint_url: url of the config
    :return:
    '''
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url=endpoint_url
    )

    with open('/tmp/config.yaml', 'w') as outfile:
        yaml.dump(config, outfile, default_flow_style=False)

    try:
        s3_client.upload_file('/tmp/config.yaml', bucket, path)
    except Exception as e:
        print(f"Request to write config file in {bucket} bucket failed due to: {e}. Retrying in {cron_interval} minutes...")

def write_metrics(metrics):
    '''
    write custom metrics in Yandex Monitoring
    :param metrics: list of metrics to write
    :return:
    '''
    try:
        r = requests.post('https://monitoring.api.cloud.yandex.net/monitoring/v2/data/write?folderId=%s&service=custom' % folder_id,  json={"metrics": metrics}, headers={'Authorization': 'Bearer %s'  % iam_token})
    except Exception as e:
        print(f"Request to write metrics failed due to: {e}. Retrying in {cron_interval} minutes...")

    if r.status_code != 200:
        print(f"Unexpected status code {r.status_code} for writing metrics. Retrying in {cron_interval} minutes...")
    if 'errorMessage' in r.json():
        print(f"Error of writing metrics. More details: {r.json()['errorMessage']}. Retrying in {cron_interval} minutes...")


def failover(route_table):
    '''
    changes next hop in route table by using REST API request to VPC API
    :param route_table: route table is dictionary with route table id, new next hop address and list of static routes
    :return:
    '''

    print(f"Updating route table {route_table['route_table_id']} with next hop address {route_table['next_hop']}. New route table: {route_table['routes']}")
    try:
        r = requests.patch('https://vpc.api.cloud.yandex.net/vpc/v1/routeTables/%s' % route_table['route_table_id'], json={"updateMask": "staticRoutes", "staticRoutes": route_table['routes'] } ,headers={'Authorization': 'Bearer %s'  % iam_token})
    except Exception as e:
        print(f"Request to update route table {route_table['route_table_id']} failed due to: {e}. Retrying in {cron_interval} minutes...")
        # add custom metric 'route_switcher.table_changed' into metric list for Yandex Monitoring that error happened during table change
        metrics.append({"name": "route_switcher.table_changed", "labels": {"route_switcher_name": function_name, "route_table_name": route_table['name'], "folder_name": folder_name}, "type": "IGAUGE", "value": 2, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})
        return

    if r.status_code != 200:
        print(f"Unexpected status code {r.status_code} for updating route table {route_table['route_table_id']}. More details: {r.json().get('message')}. Retrying in {cron_interval} minutes...")
        # add custom metric 'route_switcher.table_changed' into metric list for Yandex Monitoring that error happened during table change
        metrics.append({"name": "route_switcher.table_changed", "labels": {"route_switcher_name": function_name, "route_table_name": route_table['name'], "folder_name": folder_name}, "type": "IGAUGE", "value": 2, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})
        return

    if 'id' in r.json():
        operation_id = r.json()['id']
        print(f"Operation {operation_id} for updating route table {route_table['route_table_id']}. More details: {r.json()}")
        # add custom metric 'route_switcher.table_changed' into metric list for Yandex Monitoring about table change
        metrics.append({"name": "route_switcher.table_changed", "labels": {"route_switcher_name": function_name, "route_table_name": route_table['name'], "folder_name": folder_name}, "type": "IGAUGE", "value": 1, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})
    else:
        print(f"Failed to start operation for updating route table {route_table['route_table_id']}. Retrying in {cron_interval} minutes...")
        # add custom metric 'route_switcher.table_changed' into metric list for Yandex Monitoring that error happened during table change
        metrics.append({"name": "route_switcher.table_changed", "labels": {"route_switcher_name": function_name, "route_table_name": route_table['name'], "folder_name": folder_name}, "type": "IGAUGE", "value": 2, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})


def network_interface_update(router_network_interface):
    '''
    changes router network interface by using REST API request to Compute API
    :param router_network_interface: dictionary with router vm id, network interface index and list of security group ids
    :return: operation id for updateNetworkInterface API request
    '''

    if router_network_interface['last_operation_id']:
        # get last operations updateNetworkInterface for vm id from Compute API
        try:
            r = requests.get("https://operation.api.cloud.yandex.net/operations/%s" % router_network_interface['last_operation_id'], headers={'Authorization': 'Bearer %s'  % iam_token})
        except Exception as e:
            print(f"Request to get operation {router_network_interface['last_operation_id']} for router {router_network_interface['router_hc_address']} failed due to: {e}.")    
        if r.status_code != 200:
            print(f"Unexpected status code {r.status_code} for getting operation {router_network_interface['last_operation_id']} for router {router_network_interface['router_hc_address']}. More details: {r.json().get('message')}.")
        if 'done' in r.json() and not r.json()['done']:
            # if last operation for updating this interface is still in progress (done = false) exit from function and do not try to update network interface
            print(f"Operation id {router_network_interface['last_operation_id']} is still in progress for updating router {router_network_interface['router_hc_address']} network interface index {router_network_interface['index']}.")
            return {}
           
    print(f"Updating router {router_network_interface['router_hc_address']} network interface index {router_network_interface['index']} with security groups: {router_network_interface['security_group_ids']}")
    try:
        r = requests.patch('https://compute.api.cloud.yandex.net/compute/v1/instances/%s/updateNetworkInterface' % router_network_interface['vm_id'], json={"networkInterfaceIndex": str(router_network_interface['index']), "updateMask": "securityGroupIds", "securityGroupIds": router_network_interface['security_group_ids']} ,headers={'Authorization': 'Bearer %s'  % iam_token})
    except Exception as e:
        print(f"Request to update router {router_network_interface['router_hc_address']} network interface index {router_network_interface['index']} failed due to: {e}. Retrying in {cron_interval} minutes...")
        # add custom metric 'route_switcher.security_groups_changed' into metric list for Yandex Monitoring that error happened during security groups change for router
        metrics.append({"name": "route_switcher.security_groups_changed", "labels": {"route_switcher_name": function_name, "router_ip": router_network_interface['router_hc_address'], "interface_index": router_network_interface['index'], "folder_name": folder_name}, "type": "IGAUGE", "value": 2, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})
        return {}

    if r.status_code != 200:
        print(f"Unexpected status code {r.status_code} for updating router {router_network_interface['router_hc_address']} network interface index {router_network_interface['index']}. More details: {r.json().get('message')}. Retrying in {cron_interval} minutes...")
        # add custom metric 'route_switcher.security_groups_changed' into metric list for Yandex Monitoring that error happened during security groups change for router
        metrics.append({"name": "route_switcher.security_groups_changed", "labels": {"route_switcher_name": function_name, "router_ip": router_network_interface['router_hc_address'], "interface_index": router_network_interface['index'], "folder_name": folder_name}, "type": "IGAUGE", "value": 2, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})
        return {}

    if 'id' in r.json():
        operation_id = r.json()['id']
        print(f"Operation {operation_id} for updating router {router_network_interface['router_hc_address']} network interface index {router_network_interface['index']}. More details: {r.json()}")
        # add custom metric 'route_switcher.security_groups_changed' into metric list for Yandex Monitoring about security groups change for router
        metrics.append({"name": "route_switcher.security_groups_changed", "labels": {"route_switcher_name": function_name, "router_ip": router_network_interface['router_hc_address'], "interface_index": router_network_interface['index'], "folder_name": folder_name}, "type": "IGAUGE", "value": 1, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})
        return {'vm_id': router_network_interface['vm_id'], 'interface_index': router_network_interface['index'], 'operation_id': operation_id}
    else:
        print(f"Failed to start operation for updating router {router_network_interface['router_hc_address']} network interface index {router_network_interface['index']}. Retrying in {cron_interval} minutes...")
        # add custom metric 'route_switcher.security_groups_changed' into metric list for Yandex Monitoring that error happened during security groups change for router
        metrics.append({"name": "route_switcher.security_groups_changed", "labels": {"route_switcher_name": function_name, "router_ip": router_network_interface['router_hc_address'], "interface_index": router_network_interface['index'], "folder_name": folder_name}, "type": "IGAUGE", "value": 2, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})
        return {}


def handler(event, context):
    start_time = time.time()

    global iam_token 
    iam_token = context.token['access_token']
    global folder_id
    folder_id = event['event_metadata']['folder_id']
    global metrics

    # get route tables from VPC
    config_route_tables_routers = get_config_route_tables_and_routers()
    if config_route_tables_routers is None:
        # exit from function as some errors happened when getting route tables
        return
    
    error_message = config_route_tables_routers['error_message']
    if error_message is not None:
        print(error_message)
        # exit from function as some errors happened when getting route tables
        return
    
    all_routeTables = config_route_tables_routers['all_routeTables']
    routers = config_route_tables_routers['routers']
    function_life_time = cron_interval * 60
    checking_num = 1
    # repeat checking router status in loop 
    # checks router status and fails over if router fails
    while (time.time() - start_time) < function_life_time:
        last_check_time = time.time()
        # get latest config file from bucket
        config = get_config()
        if config is None:
            return
        if config['updating_tables'] == True:
            # current changes of next hops in route tables are still running, then wait for a timer
            current_time = time.time()       
            if (current_time - start_time + router_healthcheck_interval) < function_life_time:
                print(f"Another operation for updating route tables is running. Retrying in {router_healthcheck_interval} seconds...")
                if (current_time - last_check_time) < router_healthcheck_interval:
                    time.sleep(router_healthcheck_interval - (current_time - last_check_time))
                checking_num = checking_num + 1
                continue
            else:
                # looks like something goes wrong if during the time of cron_interval (1 min or more) another operation for updating route tables has not been completed
                # then try to update route tables once again during the next launch of function
                # set flag of updating tables as False and update config file in bucket 
                config['updating_tables'] = False
                put_config(config)
                return
        # get router status from NLB
        routerStatus = get_router_status(config)
        if routerStatus is None:
            # exit from function as some errors happened when checking router status
            return
 
        metrics = list()        
        healthy_nexthops = {}
        unhealthy_nexthops = {}
        router_vm_ids = {}
        for router in config['routers']:
            router_hc_address = router['healthchecked_ip']        
            router_interfaces = router['interfaces']
            if routerStatus[router_hc_address] != 'HEALTHY':
                # add custom metric 'route_switcher.router_state' into metric list for Yandex Monitoring that router state is not healthy
                metrics.append({"name": "route_switcher.router_state", "labels": {"router_ip": router_hc_address, "folder_name": folder_name}, "type": "IGAUGE", "value": 0, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})
                # prepare dictionary with UNHEALTHY nexthops as {key:value}, where key - nexthop address, value - nexthop address of backup router
                for interface in router_interfaces:    
                    unhealthy_nexthops[interface['own_ip']] = interface['backup_peer_ip'] 
            else:
                # add custom metric 'route_switcher.router_state' into metric list for Yandex Monitoring that router state is healthy
                metrics.append({"name": "route_switcher.router_state", "labels": {"router_ip": router_hc_address, "folder_name": folder_name}, "type": "IGAUGE", "value": 1, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})
                # prepare dictionary with HEALTHY nexthops as {key:value}, where key - nexthop address, value - nexthop address of backup router
                for interface in router_interfaces:
                    healthy_nexthops[interface['own_ip']] = interface['backup_peer_ip']
            # prepare dictionary with VM id of routers as {key:value}, where key - router healthcheck IP address, value - vm id
            if 'vm_id' in router and router['vm_id']:
                router_vm_ids[router_hc_address] = {'vm_id': router['vm_id'], 'primary': router['primary'], 'interfaces': router['interfaces']}  

        router_with_changed_status = ""
        all_modified_routeTables = list()
        for config_route_table in config['route_tables']:
            routeTable_name = all_routeTables[config_route_table['route_table_id']]['name']
            routeTable = all_routeTables[config_route_table['route_table_id']]['staticRoutes']
            routeTable_changes = {'modified':False}
            routeTable_prefixes = set()
            for ip_route in routeTable: 
                # checking if next hop is one of a router addresses
                if 'nextHopAddress' in ip_route:
                    if ip_route['nextHopAddress'] in healthy_nexthops or ip_route['nextHopAddress'] in unhealthy_nexthops:
                        # populate routeTable_prefixes set with route table prefixes
                        routeTable_prefixes.add(ip_route['destinationPrefix'])
                        if ip_route['nextHopAddress'] in unhealthy_nexthops:
                            # get primary router from config stored in bucket
                            primary_router = config_route_table['routes'][ip_route['destinationPrefix']]
                            if primary_router in healthy_nexthops and ip_route['nextHopAddress'] != primary_router: 
                                # if primary router became healthy and backup router is still used as next hop, change next hop address to primary router                     
                                if router_with_changed_status != routers[primary_router]:
                                    router_with_changed_status = routers[primary_router]
                                    print(f"Router {router_with_changed_status} became HEALTHY.")
                                ip_route.update({'nextHopAddress':primary_router})
                                routeTable_changes = {'modified':True, 'next_hop':primary_router}
                            else:
                                # if primary router is not healthy change next hop address to backup router  
                                backup_router = unhealthy_nexthops[ip_route['nextHopAddress']]                                
                                # also check whether backup router address is in healthy next hops
                                if backup_router in healthy_nexthops:
                                    if router_with_changed_status != routers[ip_route['nextHopAddress']]:
                                        router_with_changed_status = routers[ip_route['nextHopAddress']]
                                        print(f"Router {router_with_changed_status} is UNHEALTHY.")                   
                                    ip_route.update({'nextHopAddress':backup_router})
                                    routeTable_changes = {'modified':True, 'next_hop':backup_router}
                                else:
                                    print(f"Backup next hop {backup_router} is not healthy. Can not switch next hop {ip_route['nextHopAddress']} for route {ip_route['destinationPrefix']} in route table {config_route_table['route_table_id']}. Retrying in {cron_interval} minutes...")   
                        else:
                            # if route-switcher module has 'back_to_primary' input variable set as 'true' we back to primary router after its recovery
                            if back_to_primary == 'true':
                                # get primary router from config stored in bucket
                                primary_router = config_route_table['routes'][ip_route['destinationPrefix']]
                                if primary_router in healthy_nexthops and ip_route['nextHopAddress'] != primary_router: 
                                    # if primary router became healthy and backup router is still used as next hop, change next hop address to primary router                     
                                    if router_with_changed_status != routers[primary_router]:
                                        router_with_changed_status = routers[primary_router]
                                        print(f"Router {router_with_changed_status} became HEALTHY.")
                                    ip_route.update({'nextHopAddress':primary_router})
                                    routeTable_changes = {'modified':True, 'next_hop':primary_router}
                               
            if routeTable_changes['modified']:
                # if next hop for some routes was changed add this table to all_modified_routeTables list
                all_modified_routeTables.append({'route_table_id':config_route_table['route_table_id'], 'name':routeTable_name, 'next_hop':routeTable_changes['next_hop'], 'routes':routeTable})
            else:
                # add custom metric 'route_switcher.table_changed' into metric list for Yandex Monitoring that table is not changed
                metrics.append({"name": "route_switcher.table_changed", "labels": {"route_switcher_name": function_name, "route_table_name": routeTable_name, "folder_name": folder_name}, "type": "IGAUGE", "value": 0, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})
 
        
        if all_modified_routeTables: 
            # set flag of updating tables as True and update config file in bucket 
            config['updating_tables'] = True
            put_config(config)
            # add custom custom metric 'route_switcher.switchover' into metric list for Yandex Monitoring that switchover is required
            metrics.append({"name": "route_switcher.switchover", "labels": {"route_switcher_name": function_name, "folder_name": folder_name}, "type": "IGAUGE", "value": 1, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})
            # we have a list of all modified route tables 
            # create and launch a thread pool (with 8 max_workers) to execute failover function asynchronously for each modified route table    
            with pool.ThreadPoolExecutor(max_workers=8) as executer:
                try:
                    executer.map(failover, all_modified_routeTables)
                except Exception as e:
                    print(f"Request to execute failover function failed due to: {e}. Retrying in {cron_interval} minutes...")

            # set flag of updating tables as False and update config file in bucket 
            config['updating_tables'] = False
            put_config(config)

            if not router_vm_ids:
                # write metrics into Yandex Monitoring
                write_metrics(metrics)
                # exit from function as failover was executed for route tables and there are no security groups configuration for routers in configuration file
                return
        else:
            # add custom custom metric 'route_switcher.switchover' into metric list for Yandex Monitoring that switchover is not required
            metrics.append({"name": "route_switcher.switchover", "labels": {"route_switcher_name": function_name, "folder_name": folder_name}, "type": "IGAUGE", "value": 0, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})
            if not router_vm_ids:
                # write metrics into Yandex Monitoring
                write_metrics(metrics) 

        if router_vm_ids:
            primary_router_hc_address = ""
            backup_router_hc_address = ""
            all_modified_router_network_interfaces = list()
            primary_router_network_interfaces = list()
            backup_router_network_interfaces = list()
            for router_hc_address in router_vm_ids:
                if router_vm_ids[router_hc_address]['primary'] == True:
                    primary_router_hc_address = router_hc_address
                else:
                    backup_router_hc_address = router_hc_address
                
            if routerStatus[primary_router_hc_address] != 'HEALTHY':
                if routerStatus[backup_router_hc_address] == 'HEALTHY':
                    # if primary router is not healthy and backup router is healthy
                    # prepare list of primary router network interfaces for updating security groups with security groups of backup router from configuration file 
                    primary_router_network_interfaces = get_diff_security_groups(router_vm_ids[primary_router_hc_address]['vm_id'], primary_router_hc_address, router_vm_ids[backup_router_hc_address]['interfaces'])
                    if primary_router_network_interfaces:
                        all_modified_router_network_interfaces.extend(primary_router_network_interfaces)
                    # prepare list of backup router network interfaces for updating security groups with security groups of primary router from configuration file
                    backup_router_network_interfaces = get_diff_security_groups(router_vm_ids[backup_router_hc_address]['vm_id'], backup_router_hc_address, router_vm_ids[primary_router_hc_address]['interfaces'])
                    if backup_router_network_interfaces:
                        all_modified_router_network_interfaces.extend(backup_router_network_interfaces)
            else:
                if routerStatus[backup_router_hc_address] == 'HEALTHY':
                    if back_to_primary == 'true':
                        # if primary router is healthy and backup router is healthy and back_to_primary == 'true'
                        # prepare list of primary router network interfaces for updating security groups with security groups of primary router from configuration file 
                        primary_router_network_interfaces = get_diff_security_groups(router_vm_ids[primary_router_hc_address]['vm_id'], primary_router_hc_address, router_vm_ids[primary_router_hc_address]['interfaces'])
                        if primary_router_network_interfaces:
                            all_modified_router_network_interfaces.extend(primary_router_network_interfaces)
                        # prepare list of backup router network interfaces for updating security groups with security groups of backup router from configuration file
                        backup_router_network_interfaces = get_diff_security_groups(router_vm_ids[backup_router_hc_address]['vm_id'], backup_router_hc_address, router_vm_ids[backup_router_hc_address]['interfaces'])
                        if backup_router_network_interfaces:
                            all_modified_router_network_interfaces.extend(backup_router_network_interfaces)
                    else:
                        # if primary router is healthy and backup router is healthy and back_to_primary == 'false'
                        # check if backup router has primary security groups currently
                        backup_router_network_interfaces = get_diff_security_groups(router_vm_ids[backup_router_hc_address]['vm_id'], backup_router_hc_address, router_vm_ids[primary_router_hc_address]['interfaces'])
                        if backup_router_network_interfaces:
                            # backup router does not have primary security groups currently
                            # prepare list of primary router network interfaces for updating security groups with security groups of primary router from configuration file 
                            primary_router_network_interfaces = get_diff_security_groups(router_vm_ids[primary_router_hc_address]['vm_id'], primary_router_hc_address, router_vm_ids[primary_router_hc_address]['interfaces'])
                            if primary_router_network_interfaces:
                                all_modified_router_network_interfaces.extend(primary_router_network_interfaces)
                        else:
                            # prepare list of primary router network interfaces for updating security groups with security groups of backup router from configuration file 
                            primary_router_network_interfaces = get_diff_security_groups(router_vm_ids[primary_router_hc_address]['vm_id'], primary_router_hc_address, router_vm_ids[backup_router_hc_address]['interfaces'])
                            if primary_router_network_interfaces:
                                all_modified_router_network_interfaces.extend(primary_router_network_interfaces)
                else:
                    # if primary router is healthy and backup router is not healthy
                    # prepare list of primary router network interfaces for updating security groups with security groups of primary router from configuration file 
                    primary_router_network_interfaces = get_diff_security_groups(router_vm_ids[primary_router_hc_address]['vm_id'], primary_router_hc_address, router_vm_ids[primary_router_hc_address]['interfaces'])
                    if primary_router_network_interfaces:
                        all_modified_router_network_interfaces.extend(primary_router_network_interfaces)
                    # prepare list of backup router network interfaces for updating security groups with security groups of backup router from configuration file
                    backup_router_network_interfaces = get_diff_security_groups(router_vm_ids[backup_router_hc_address]['vm_id'], backup_router_hc_address, router_vm_ids[backup_router_hc_address]['interfaces'])
                    if backup_router_network_interfaces:
                        all_modified_router_network_interfaces.extend(backup_router_network_interfaces)


            if all_modified_router_network_interfaces:
                # update security groups for router network interfaces  
                operation_results = list()
                with pool.ThreadPoolExecutor(max_workers=8) as executer:
                    try:
                        # launch execution of updating router network interfaces and receiving return results of function 'network_interface_update' 
                        operation_results = list(executer.map(network_interface_update, all_modified_router_network_interfaces))
                    except Exception as e:
                        print(f"Request to execute network_interface_update function failed due to: {e}. Retrying in {cron_interval} minutes...")  
                operation_counter = 0
                if operation_results:
                    # update config file in bucket with operation id of updateNetworkInterface API request for each router interfaces beeing updated                  
                    for operation in operation_results:
                        if 'vm_id' in operation and operation['vm_id']:
                            for router in config['routers']:
                                if 'vm_id' in router and router['vm_id'] and router['vm_id'] == operation['vm_id']:
                                    for router_interface in router['interfaces']:
                                        if router_interface['index'] == operation['interface_index']: 
                                            router_interface.update({'last_operation_id': operation['operation_id']})
                                            operation_counter += 1 
                # write metrics into Yandex Monitoring
                write_metrics(metrics)
                if operation_counter:
                    # update config file in bucket with operation id of updateNetworkInterface API requests
                    put_config(config)
                    # exit from function as update for security groups was executed
                    return
            else:
                # add custom metric 'route_switcher.security_groups_changed' into metric list for Yandex Monitoring that security group is not changed for routers
                for router_config_interface in router_vm_ids[primary_router_hc_address]['interfaces']:
                    if router_config_interface['index'] is not None:
                        metrics.append({"name": "route_switcher.security_groups_changed", "labels": {"route_switcher_name": function_name, "router_ip": primary_router_hc_address, "interface_index": router_config_interface['index'], "folder_name": folder_name}, "type": "IGAUGE", "value": 0, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})
                for router_config_interface in router_vm_ids[backup_router_hc_address]['interfaces']:
                    if router_config_interface['index'] is not None:
                        metrics.append({"name": "route_switcher.security_groups_changed", "labels": {"route_switcher_name": function_name, "router_ip": backup_router_hc_address, "interface_index": router_config_interface['index'], "folder_name": folder_name}, "type": "IGAUGE", "value": 0, "ts": str(datetime.datetime.now(datetime.timezone.utc).isoformat())})
                # write metrics into Yandex Monitoring
                write_metrics(metrics)

        current_time = time.time()      
        if (current_time - start_time + router_healthcheck_interval) < function_life_time:
            if (current_time - last_check_time) < router_healthcheck_interval:
                time.sleep(router_healthcheck_interval - (current_time - last_check_time))
            checking_num = checking_num + 1
        else:
            break
    
    