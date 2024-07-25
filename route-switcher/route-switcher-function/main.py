import yaml
import os
import boto3
import requests
import concurrent.futures as pool
import time

iam_token = ""
endpoint_url='https://storage.yandexcloud.net'
path = os.getenv('CONFIG_PATH')
bucket = os.getenv('BUCKET_NAME')
cron_interval = os.getenv('CRON_INTERVAL')
back_to_primary = os.getenv('BACK_TO_PRIMARY').lower()
router_healthcheck_interval = os.getenv('ROUTER_HCHK_INTERVAL')

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
                return
            return targetStatus 
    else:
        print(f"There are no target endpoints in load balancer {config['loadBalancerId']}. Please add two endpoints. Retrying in {cron_interval} minutes...")
        return    


def get_config_route_tables_and_routers():
    '''
    get config in special format from bucket
    get actual routes from route tables in VPC which are protected by route-switcher 
    :param config: configuration dictionary with route tables and load balancer id
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
                # prepare dictionary with router nexthops as {key:value}, where key - nexthop address, value - nexthop address of backup router
                if 'own_ip' in interface and interface['own_ip']:
                    if 'backup_peer_ip' in interface and interface['backup_peer_ip']:
                        nexthops[interface['own_ip']] = interface['backup_peer_ip']
                        # prepare dictionary with router healthcheck IP addresses as {key:value}, where key - nexthop address, value - router healthcheck IP address of this nexthop address
                        routers[interface['own_ip']] = router_hc_address      
                    else:
                        print(f"Router {router_hc_address} does not have 'backup_peer_ip' configuration for interface. Please add 'backup_peer_ip' value in 'interfaces' input variable for Terraform route-switcher module.")
                        router_error = True
                        continue
                else:
                    print(f"Router {router_hc_address} does not have 'own_ip' configuration for interface. Please add 'own_ip' value in 'interfaces' input variable for Terraform route-switcher module.")
                    router_error = True
                    continue  
        else:
            print(f"Router {router_hc_address} is not in target endpoints of load balancer {config['loadBalancerId']}. Please check load balancer configuration or 'routers' input variable for Terraform route-switcher module.")
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
            all_routeTables.update({config_route_table['route_table_id']:sorted(routeTable, key=lambda i: i['destinationPrefix'])})

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

    s3_client.upload_file('/tmp/config.yaml', bucket, path)


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
        return

    if r.status_code != 200:
        print(f"Unexpected status code {r.status_code} for updating route table {route_table['route_table_id']}. More details: {r.json().get('message')}. Retrying in {cron_interval} minutes...")
        return

    if 'id' in r.json():
        operation_id = r.json()['id']
        print(f"Operation {operation_id} for updating route table {route_table['route_table_id']}. More details: {r.json()}")
    else:
        print(f"Failed to start operation for updating route table {route_table['route_table_id']}. Retrying in {cron_interval} minutes...")


def handler(event, context):
    start_time = time.time()

    global iam_token 
    iam_token = context.token['access_token']

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
    
    config = config_route_tables_routers['config']
    all_routeTables = config_route_tables_routers['all_routeTables']
    routers = config_route_tables_routers['routers']
    function_life_time = int(cron_interval) * 60
    checking_num = 1
    # repeat checking router status in loop 
    # checks router status and fails over if router failed
    while (time.time() - start_time) < function_life_time:
        # get latest config file from bucket
        config = get_config()
        last_check_time = time.time() - start_time
        if config is None:
            return
        if config['updating_tables'] == True:
            # current changes of next hops in route tables are still running, then wait for a timer
            if (last_check_time + int(router_healthcheck_interval)) < function_life_time:
                print(f"Another operation for updating route tables is running. Retrying in {router_healthcheck_interval} seconds...")
                time.sleep(checking_num * int(router_healthcheck_interval) - last_check_time)
                checking_num = checking_num + 1
                continue
            else:
                # looks like something goes wrong if during the time of cron_interval (1 min or more) another operation for updating route tables has not been completed
                # then try to update route tables once again during the next launch of function
                # set flag of updating tables as False and update config file in bucket 
                config['updating_tables'] = False
                put_config(config)
                break

        # get router status from NLB
        routerStatus = get_router_status(config)
        if routerStatus is None:
            # exit from function as some errors happened when checking router status
            return
                
        healthy_nexthops = {}
        unhealthy_nexthops = {}
        for router in config['routers']:
            router_hc_address = router['healthchecked_ip']        
            router_interfaces = router['interfaces']
            if routerStatus[router_hc_address] != 'HEALTHY':
                # prepare dictionary with UNHEALTHY nexthops as {key:value}, where key - nexthop address, value - nexthop address of backup router
                for interface in router_interfaces:    
                    unhealthy_nexthops[interface['own_ip']] = interface['backup_peer_ip']    
            else:
                # prepare dictionary with HEALTHY nexthops as {key:value}, where key - nexthop address, value - nexthop address of backup router
                for interface in router_interfaces:
                    healthy_nexthops[interface['own_ip']] = interface['backup_peer_ip']
        
        router_with_changed_status = ""
        all_modified_routeTables = list()
        for config_route_table in config['route_tables']:
            routeTable = all_routeTables[config_route_table['route_table_id']]
            routeTable_changes = {'modified':False}
            routeTable_prefixes = set()
            for ip_route in routeTable: 
                # checking if next hop is one of a router addresses
                if ip_route['nextHopAddress'] in healthy_nexthops or ip_route['nextHopAddress'] in unhealthy_nexthops:
                    # populate routeTable_prefixes set with route table prefixes
                    routeTable_prefixes.add(ip_route['destinationPrefix'])
                    if ip_route['nextHopAddress'] in unhealthy_nexthops:
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
                all_modified_routeTables.append({'route_table_id':config_route_table['route_table_id'], 'next_hop':routeTable_changes['next_hop'], 'routes':routeTable})
 
        
        if all_modified_routeTables: 
            # set flag of updating tables as True and update config file in bucket 
            config['updating_tables'] = True
            put_config(config)
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
            # exit from function as failover was executed for route tables
            return
               
        if (last_check_time + int(router_healthcheck_interval)) < function_life_time:
            time.sleep(checking_num * int(router_healthcheck_interval) - last_check_time)
            checking_num = checking_num + 1
        else:
            break

    

