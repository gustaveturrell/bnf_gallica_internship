import os
import sys
import json
import argparse
from tqdm import tqdm
import datetime 
from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, MetadataReader


oai_dc_reader = MetadataReader(
    fields={
    'title':       ('textList', 'oai_dc:dc/dc:title/text()'),
    'creator':     ('textList', 'oai_dc:dc/dc:creator/text()'),
    'subject':     ('textList', 'oai_dc:dc/dc:subject/text()'),
    'description': ('textList', 'oai_dc:dc/dc:description/text()'),
    'publisher':   ('textList', 'oai_dc:dc/dc:publisher/text()'),
    'contributor': ('textList', 'oai_dc:dc/dc:contributor/text()'),
    'date':        ('textList', 'oai_dc:dc/dc:date/text()'),
    'type':        ('textList', 'oai_dc:dc/dc:type[@xml:lang="fre"]/text()'),
    'format':      ('textList', 'oai_dc:dc/dc:format/text()'),
    'identifier':  ('textList', 'oai_dc:dc/dc:identifier/text()'),
    'source':      ('textList', 'oai_dc:dc/dc:source/text()'),
    'language':    ('textList', 'oai_dc:dc/dc:language/text()'),
    'relation':    ('textList', 'oai_dc:dc/dc:relation/text()'),
    'coverage':    ('textList', 'oai_dc:dc/dc:coverage/text()'),
    'rights':      ('textList', 'oai_dc:dc/dc:rights[@xml:lang="fre"]/text()')
    },
    namespaces={
    'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
    'dc' : 'http://purl.org/dc/elements/1.1/'}
    )


def request(client, prefix, setspec, filename, directory='./', mb=20):
    #//////////////////////////////////////////////////////////////////////////
    try:
        #[init]
        width = 10 
        nbatch = 0
        totalsize = 0
        errorscatch = 0
        recordsdownload = 0
        batchsize = mb * 1024 * 1024
        #[setting parameters for oaipmh]
        registry = MetadataRegistry()
        registry.registerReader(prefix, oai_dc_reader)
        client = Client(client, registry)
        #//////////////////////////////////////////////////////////////////////////
        #[display information, sys.stderr]
        identify = client.identify()
        print("~  [Repository name]   ~  \n{0}".format(identify.repositoryName()), file=sys.stderr)
        print("~  [Base URL]          ~  \n{0}".format(identify.baseURL()), file=sys.stderr)
        print("~  [Protocol version]  ~  \n{0}".format(identify.protocolVersion()), file=sys.stderr)
        print("~  [Granularity]       ~  \n{0}".format(identify.granularity()), file=sys.stderr)
        print("~  [Compression]       ~  \n{0}".format(identify.compression()), file=sys.stderr)
        print("~  [Deleted record]    ~  \n{0}".format(identify.deletedRecord()), file=sys.stderr)
        print("~  [Metadata Formats]  ~  \n{0}".format(client.listMetadataFormats()), file=sys.stderr)
        print(f"\nSTART|{datetime.datetime.now()}",file=sys.stderr)
        #//////////////////////////////////////////////////////////////////////////
        logfile = open(os.path.join(directory, f'{filename}_log.txt'), 'w', encoding='utf-8')
        logfile.write(f"START|{datetime.datetime.now()}\n")
        #//////////////////////////////////////////////////////////////////////////
    	#[open file for writing]
        output = open(os.path.join(directory,f'{filename}_{prefix}_{mb}mb_batch_{nbatch}.json'), 'w', encoding='utf-8')
        #[wrap the beginning and append all the records in `data`]
        output.write('{\n"data":[')
        #[flag for adding comma]
        firstrecord = True  
        #//////////////////////////////////////////////////////////////////////////
        for record in tqdm(client.listRecords(metadataPrefix=prefix, set=setspec),file=sys.stdout,
                            total=None,
                            desc='Downloading records',
                            unit='record'):
            
            header, metadata, _ = record
            #[couting records]
            recordsdownload += 1
            #[counting errors]
            if metadata is None:
                errorscatch +=1
                logfile.write(f"ERROR|{datetime.datetime.now()} for {header.identifier()}\n")
                continue

            #[retrieve metadata in dictionnary form]
            dictionnary = metadata.getMap()
            #[get the size for the records]
            recordsize = len(json.dumps(dictionnary).encode('utf-8'))

            #//////////////////////////////////////////////////////////////////////////
            #[check if json file is less than the bachsize]
            if totalsize + recordsize > batchsize:
                #[close the list and ending of wrap]
                output.write(']\n}')
                output.close()
                nbatch += 1
                totalsize = 0
                #[open file for writing]
                output = open(os.path.join(directory,f'{filename}_{prefix}_{mb}mb_batch_{nbatch}.json'), 'w', encoding='utf-8')
                output.write('{\n"data":[')
                firstrecord = True
            #//////////////////////////////////////////////////////////////////////////

            #[writing records]
            if not firstrecord:
                output.write(',\n')
            else:
                firstrecord = False

            json.dump(dictionnary, output, ensure_ascii=False)

            #[increment the totalsize]
            totalsize += recordsize

        output.write(']}')
        logfile.write(f"END|{datetime.datetime.now()}\nRECORDS|{recordsdownload}\nERRORS|{errorscatch}\n")
        logfile.close()
        output.close()
    except KeyboardInterrupt:
        print(f'END|{datetime.datetime.now()}',file=sys.stderr)
        output.write(']}')
        output.close()
        logfile.write(f"END|{datetime.datetime.now()}\nRECORDS|{recordsdownload}, ERRORS|{errorscatch}\n")
        sys.exit(1)


#//////////////////////////////////////////////////////////////////////////
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
