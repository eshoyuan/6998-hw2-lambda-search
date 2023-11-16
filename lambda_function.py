import json
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError

REGION = 'us-east-1'
# HOST = 'search-photos-dhul7pceqzrodplgllu4qk25jy.us-east-1.es.amazonaws.com/'
Host = 'search-photos1-sxipfgbn7hxqx3ibsnr3lvxany.us-east-1.es.amazonaws.com/'
INDEX = 'photos11111'
es_client = boto3.client('opensearch')
client = boto3.client('lexv2-runtime')
def lambda_handler(event, context):
    # msg_from_user = event['messages'][0]['unstructured']['text']
    print(event)
    urls = []
    # delete_all_queries()
    msg_from_user = event["queryStringParameters"]["q"]
    response = client.recognize_text(
        botId='VWR9UBVFEB', # MODIFY HERE
        botAliasId='XOM4DOAVBK', # MODIFY HERE
        localeId='en_US',
        sessionId='testuser',
        text=msg_from_user)
    msg_from_lex = response.get('messages')
    if msg_from_lex is None:
        return {
        'statusCode': 200,
        'body': json.dumps(urls),
        "headers": {
                "Content-Type": 'application/json',
                "Access-Control-Allow-Headers": '*',
                "Access-Control-Allow-Origin": '*',
                "Access-Control-Allow-Methods": '*',
        }
    }
    labels = msg_from_lex[0]['content'].split(',')
    # print(msg_from_lex['content'].split(','))
    print(labels)
    result = []
    if labels[1] == 'None':
        # we only have one label.
        result = query(labels[0])
        print("go from here.")
    else:
        # we have two labels.
        result_1 = query(labels[0])
        result_2 = query(labels[1])
        for r in result_2:
            if r not in result_1:
                result_1.append(r)
        result = result_1
    
    print(result)
    # from the result, get the object key and bucket_name, and make them as one url. response to frontend then.
    # next step is return some urls. and the front end will receive
    for r in result:
        url = "https://"
        url = url + r['bucket']
        url = url + ".s3.amazonaws.com/"
        url = url + r['objectKey']
        urls.append(url)
    
    return {
        'statusCode': 200,
        'body': json.dumps(urls),
        "headers": {
                "Content-Type": 'application/json',
                "Access-Control-Allow-Headers": '*',
                "Access-Control-Allow-Origin": '*',
                "Access-Control-Allow-Methods": '*',
        }
    }




def query(term):
    q = {'size': 3, 'query': {'multi_match': {'query': term}}}

    client = OpenSearch(hosts=[{
        'host': es_client.describe_domain(DomainName=INDEX)['DomainStatus']['Endpoint'],
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)

    res = client.search(index=INDEX, body=q)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])
        print(hit)
    
    return results

def delete_all_queries():
    client = OpenSearch(hosts=[{
        'host': es_client.describe_domain(DomainName=INDEX)['DomainStatus']['Endpoint'],
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)
    client.delete(
        index = 'photos111',
        id = 'YpkSeYsBK-1r4FWpLQOJ'
        )
    
    
def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)