"""Isolated official Kafka + exact OffsetCommit-receipt loss; no shared restarts."""
import argparse
import asyncio
import os
from pathlib import Path
import struct
import subprocess
import uuid


async def inside(root):
    root.mkdir(parents=True, exist_ok=True)
    for name in ('hold-commit','commit-seen'):
        (root/name).unlink(missing_ok=True)
    for attempt in range(300):
        try:
            _,ready=await asyncio.open_connection('127.0.0.1',9092);ready.close();await ready.wait_closed();break
        except OSError:
            if attempt==299:raise
            await asyncio.sleep(.1)
    tasks, writers = set(), set()
    async def connection(reader, writer):
        task = asyncio.current_task(); tasks.add(task); writers.add(writer)
        upstream = None; children = []
        requests = {}; held = []
        async def send_requests():
            while True:
                size = await reader.readexactly(4); length = struct.unpack('>i',size)[0]
                assert 8 <= length <= 32*1024*1024
                data = await reader.readexactly(length)
                api, _version, correlation = struct.unpack('>hhi',data[:8])
                requests[correlation]=api
                if api==8 and (root/'hold-commit').exists(): (root/'commit-seen').write_text('forwarded\n')
                upstream.write(size+data);await upstream.drain()
        async def responses(remote):
            while True:
                size=await remote.readexactly(4);length=struct.unpack('>i',size)[0]
                assert 4<=length<=32*1024*1024
                data=await remote.readexactly(length)
                correlation=struct.unpack('>i',data[:4])[0]
                if requests.pop(correlation,None)==8 and (root/'hold-commit').exists(): held.append(size+data)
                else: writer.write(size+data);await writer.drain()
        async def release():
            while True:
                await asyncio.sleep(.005)
                if not (root/'hold-commit').exists():
                    while held: writer.write(held.pop(0))
                    await writer.drain()
        try:
            remote,upstream=await asyncio.open_connection('127.0.0.1',9092);writers.add(upstream)
            children=[asyncio.create_task(send_requests()),asyncio.create_task(responses(remote)),asyncio.create_task(release())]
            await asyncio.wait(children,return_when=asyncio.FIRST_COMPLETED)
        except (OSError,asyncio.IncompleteReadError): pass
        finally:
            for child in children: child.cancel()
            await asyncio.gather(*children,return_exceptions=True)
            for stream in (writer,upstream):
                if stream:stream.close();writers.discard(stream)
            tasks.discard(task)
    server=await asyncio.start_server(connection,'127.0.0.1',19092)
    try:
        env={**os.environ,'SA_TEST_KAFKA_BROKERS':'127.0.0.1:19092','SA_KAFKA_FAULT_DIR':str(root)}
        process=await asyncio.create_subprocess_exec('cargo','test','--offline','--test','kafka_checkpoint_native','-j2','--','--ignored','--nocapture','--test-threads=1',env=env)
        try: assert await asyncio.wait_for(process.wait(),180)==0
        finally:
            if process.returncode is None:process.kill();await process.wait()
    finally:
        server.close();await server.wait_closed()
        for writer in tuple(writers):writer.close()
        for task in tuple(tasks):task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--inside',action='store_true');parser.add_argument('--artifacts',required=True)
    args=parser.parse_args();root=Path(args.artifacts).resolve();root.mkdir(parents=True,exist_ok=True)
    if args.inside:asyncio.run(inside(root));return
    sdk=Path(__file__).resolve().parents[1];broker='rust-kafka-commit-'+uuid.uuid4().hex
    settings={'KAFKA_NODE_ID':'1','KAFKA_PROCESS_ROLES':'broker,controller',
      'KAFKA_LISTENERS':'BROKER://:9092,CONTROLLER://:9093','KAFKA_ADVERTISED_LISTENERS':'BROKER://127.0.0.1:19092',
      'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP':'BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT','KAFKA_INTER_BROKER_LISTENER_NAME':'BROKER',
      'KAFKA_CONTROLLER_LISTENER_NAMES':'CONTROLLER','KAFKA_CONTROLLER_QUORUM_VOTERS':'1@127.0.0.1:9093',
      'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR':'1','KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR':'1',
      'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR':'1','KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS':'0'}
    command=['docker','run','-d','--name',broker]
    for key,value in settings.items():command+=['-e',key+'='+value]
    command+=['apache/kafka:4.1.2']
    try:
        subprocess.run(command,check=True,capture_output=True)
        command=['docker','run','--rm','--network','container:'+broker,'-v',str(sdk)+':/workspace',
                 '-v','rustservicelib-cargo-registry:/usr/local/cargo/registry','-v','rust-nats-target:/workspace/target',
                 '-v',str(root)+':/evidence','rustservicelib-toolchain:local','python3','/workspace/tests/kafka_commit_gate.py','--inside','--artifacts','/evidence/fault']
        with (root/'acceptance.log').open('w') as log:
            result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,timeout=240,check=False)
        assert result.returncode==0,f'Kafka gate failed: {root}/acceptance.log'
        print(f'PASS Kafka native checkpoint/commit gate: {root}')
    finally:
        log=subprocess.run(['docker','logs',broker],capture_output=True,text=True,check=False)
        (root/'broker.log').write_text(log.stdout+log.stderr)
        subprocess.run(['docker','rm','-f',broker],capture_output=True,check=False)

if __name__=='__main__':main()
