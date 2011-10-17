package org.neo4j;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.Random;

public class SimpleWriteSyncTest
{
    private static int RECORD_SIZE = 33;

    private static final Random r = new Random( System.currentTimeMillis() );

    public static void main( String args[] ) throws Exception
    {
        if ( args.length < 2 )
        {
            System.out.println( "Usage: <large file> <log file> <[record size] " + 
                    "[min tx size] [max tx size] [tx count] <[--nosync | " + 
                    "--nowritelog | --nowritestore | --noread | --nomemorymap]>>" );
            return;
        }
        FileChannel largeFile = new RandomAccessFile( args[0], "rw" ).getChannel();
        FileChannel logFile = new RandomAccessFile( args[1], "rw" ).getChannel();
        if ( args.length > 2 )
        {
            RECORD_SIZE = Integer.parseInt( args[2] );
        }
        int minTxSize = 50;
        int txCount = 10;
        int maxTxSize = 100;
        if ( args.length > 3 )
        {
            minTxSize = Integer.parseInt( args[3] );
            maxTxSize = Integer.parseInt( args[4] );
            txCount = Integer.parseInt( args[5] );
        }
        boolean sync = true;
        boolean read = true;
        boolean writeLog = true;
        boolean writeStore = true;
        boolean memoryMap = true;
        if ( args.length > 6 )
        {
            for ( int i = 6; i < args.length; i++ )
            {
                if ( args[i].equals( "--nosync" ) )
                {
                    sync = false;
                }
                else if ( args[i].equals( "--nowritelog" ) )
                {
                    writeLog = false;
                }
                else if ( args[i].equals( "--nowritestore" ) )
                {
                    writeStore = false;
                }
                else if ( args[i].equals( "--noread" ) )
                {
                    read = false;
                }
                else if ( args[i].equals( "--nomemorymap" ) )
                {
                    memoryMap = false;
                }
            }
        }
        ByteBuffer storeBuffer = ByteBuffer.allocateDirect( RECORD_SIZE );
        ByteBuffer logBuffer = ByteBuffer.allocateDirect( RECORD_SIZE );
        ByteBuffer mappedBuffer = null;
        if ( memoryMap )
        {
            storeBuffer = largeFile.map( MapMode.READ_WRITE, 0, largeFile.size() );
            logBuffer = logFile.map( MapMode.READ_WRITE, 0, maxTxSize * RECORD_SIZE * txCount );
        }
        int recordCount = (int) ( largeFile.size() / RECORD_SIZE );
        long start = System.currentTimeMillis();
        int count = 0;
        int bytesRead = 0;
        int bytesWritten = 0;
        int fdatasyncCount = 0;
        int totalRecordsTouched = 0;
        for ( int itr = 0; itr < txCount; itr++ )
        {
            int txSize = minTxSize + r.nextInt( maxTxSize - minTxSize );
            Record[] records = new Record[txSize];
            long[] positions = new long[txSize];
            for ( int i = 0; i < txSize; i++ )
            {
                positions[i] = r.nextInt( recordCount ) * RECORD_SIZE;
                records[i] = new Record();
                if ( read )
                {
                    if ( !memoryMap )
                    {
                        largeFile.position( positions[i] );
                        storeBuffer.clear();
                        bytesRead += largeFile.read( storeBuffer );
                        storeBuffer.flip();
                        storeBuffer.get( records[i].data );
                    }
                    else
                    {
                        int pos = (int) positions[i];
                        storeBuffer.position( pos );
                        storeBuffer.get( records[i].data );
                        bytesRead += storeBuffer.position() - pos;
                    }
                }
                else
                {
                    r.nextBytes( records[i].data );
                }
                totalRecordsTouched++;
            }
            Arrays.sort( positions );
            if ( writeLog )
            {
                for ( int i = 0; i < txSize; i++ )
                {
                    if (!memoryMap )
                    {
                        logBuffer.clear();
                        logBuffer.put( records[i].data );
                        logBuffer.flip();
                        bytesWritten += logFile.write( logBuffer );
                    }
                    else
                    {
                        int pos = logBuffer.position();
                        logBuffer.put( records[i].data );
                        bytesWritten += (logBuffer.position() - pos);
                    }
                }
            }
            if ( sync )
            {
                if ( !memoryMap )
                {
                    logFile.force( false );
                }
                else
                {
                    ((MappedByteBuffer) logBuffer).force();
                    logFile.force( false );
                }
                fdatasyncCount++;
            }
            if ( writeStore )
            {
                for ( int i = 0; i < txSize; i++ )
                {
                    if ( !memoryMap )
                    {
                        storeBuffer.clear();
                        storeBuffer.put( records[i].data );
                        storeBuffer.flip();
                        bytesWritten += largeFile.position( positions[i] ).write( storeBuffer );
                    }
                    else
                    {
                        int pos = (int) positions[i];
                        storeBuffer.position( pos );
                        storeBuffer.put( records[i].data );
                        bytesWritten += storeBuffer.position() - pos;
                    }
                }
            }
            count ++;
        }
        long time = System.currentTimeMillis() - start;
        float s = time / 1000.0f;
        float kb = 1024.0f;
        float mb = 1024.0f * 1024.0f;
        System.out.println( "tx_count[" + count + "] records[" + totalRecordsTouched +   
                "] fdatasyncs[" + fdatasyncCount + "] read[" + bytesRead / mb + 
                " MB] wrote[" + bytesWritten / mb + " MB]" );
        System.out.println( "Time was: " + s );
        System.out.println( count / s + " tx/s, " + totalRecordsTouched / s + 
                " records/s, " +  fdatasyncCount / s + " fdatasyncs/s, " + 
                (bytesRead / kb / s) + " kB/s on reads, " + 
                (bytesWritten / kb / s) + " kB/s on writes" );
    }

    private static class Record
    {
        byte[] data = new byte[RECORD_SIZE];
    }
}