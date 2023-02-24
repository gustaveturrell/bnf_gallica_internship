import os
import sys
import json
import argparse
from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry,oai_dc_reader
from tqdm import tqdm





def request(client, prefix, setspec, filename, directory='./', mb=20):

    #[init]
    nbatch = 0
    totalsize = 0
    batchsize = mb * 1024 * 1024
    records_downloaded = 0

    #[setting parameters for oaipmh]
    registry = MetadataRegistry()
    registry.registerReader(prefix, oai_dc_reader)
    client = Client(client, registry)


    #[display information]
    identify = client.identify()
    print("[Repository name] {0}".format(identify.repositoryName()), file=sys.stderr)
    print("[Base URL] {0}".format(identify.baseURL()), file=sys.stderr)
    print("[Protocol version] {0}".format(identify.protocolVersion()), file=sys.stderr)
    print("[Granularity] {0}".format(identify.granularity()), file=sys.stderr)
    print("[Compression] {0}".format(identify.compression()), file=sys.stderr)
    print("[Deleted record] {0}".format(identify.deletedRecord()), file=sys.stderr)
    print("[Metadata Formats] {0}".format(client.listMetadataFormats()), file=sys.stderr)

	#[open file for writing]
    output = open(os.path.join(directory,f'{filename}_{prefix}_{mb}mb_batch_{nbatch}.json'), 'w', encoding='utf-8')
    output.write('{\n"data":[')
    firstrecord = True  

    for record in tqdm(client.listRecords(metadataPrefix=prefix, set=setspec),file=sys.stdout,
                        total=None,
                        desc='Downloading records',
                        unit='record'):
        
        header, metadata, _ = record
        dictionnary = metadata.getMap()
        recordsize = len(json.dumps(dictionnary).encode('utf-8'))

        if totalsize + recordsize > batchsize:
            output.write(']\n}')
            output.close()
            nbatch += 1
            totalsize = 0
            output = open(os.path.join(directory,f'{filename}_{prefix}_{mb}mb_batch_{nbatch}.json'), 'w', encoding='utf-8')
            output.write('{\n"data":[')
            firstrecord = True


        #[writing records]
        if not firstrecord:
            output.write(',\n')
        else:
            firstrecord = False

        json.dump(dictionnary, output, ensure_ascii=False)

        
        #[Set the size]
        totalsize += recordsize
 
        
    output.write(']}')
    output.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download OAI-PMH records')
    parser.add_argument('--client', type=str, help='OAI-PMH endpoint URL', required=True)
    parser.add_argument('--prefix', type=str, help='Metadata prefix', required=True)
    parser.add_argument('--setspec', type=str, help='Set specification', required=True)
    parser.add_argument('--filename', type=str, help='Output filename', required=True)
    parser.add_argument('--directory', type=str, help='Output directory', default='./')
    parser.add_argument('--mb', type=int, help='Batch size in MB', default=20)
    args = parser.parse_args()

    request(args.client, args.prefix, args.setspec, args.filename, args.directory, args.mb)

    