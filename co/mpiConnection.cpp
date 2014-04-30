
/* Copyright (c) 2014, Carlos Duelo <cduelo@cesvima.upm.es>
 *
 * This file is part of Collage <https://github.com/Eyescale/Collage>
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License version 2.1 as published
 * by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#include "mpiConnection.h"
#include "connectionDescription.h"
#include "global.h"
#include "eventConnection.h"

#include <lunchbox/monitor.h>
#include <lunchbox/scopedMutex.h>
#include <lunchbox/mtQueue.h>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread/thread.hpp>

#include <set>
#include <queue>

namespace
{

typedef lunchbox::RefPtr< co::EventConnection > EventConnectionPtr;

/*
 * Every connection inside a process has a unique MPI tag.
 * This class allow to register a MPI tag and get a new unique.
 *
 * Due to, the tag is defined by a 16 bits integer on 
 * ConnectionDescription ( the real name is port but it
 * is reused for this purpuse ). The listeners always
 * use tags [ 0, 65535 ] and others [ 65536, 2147483648 ].
 */
static class TagManager
{

public:
TagManager() :
    _nextTag( 65536 )
{
}

bool registerTag(uint32_t tag)
{
    lunchbox::ScopedMutex< > mutex( _lock );

    if( _tags.find( tag ) != _tags.end( ) )
        return false;

    _tags.insert( tag );

    return true;
}

void deregisterTag(uint32_t tag)
{
    lunchbox::ScopedMutex< > mutex( _lock );

    /** Ensure deregister tag is a register tag. */
    LBASSERT( _tags.find( tag ) != _tags.end( ) );

    _tags.erase( tag );
}

uint32_t getTag()
{
    lunchbox::ScopedMutex< > mutex( _lock );

    do
    {
        _nextTag++;
        LBASSERT( _nextTag < 2147483648 )
    }
    while( _tags.find( _nextTag ) != _tags.end( ) );

    _tags.insert( _nextTag );

    return _nextTag;
}

private:
    std::set< uint32_t >    _tags;
    uint32_t                _nextTag;
    lunchbox::Lock          _lock;
} tagManager;

struct Petition
{
    int64_t bytes;
    void *  data;
};

class Dispatcher : lunchbox::Thread
{

public:
Dispatcher(int32_t rank, int32_t source, int32_t tag, int32_t tagClose, EventConnectionPtr notifier) :
      _rank( rank )
    , _source( source )
    , _tag( tag )
    , _tagClose( tagClose )
    , _notifier( notifier )
	, _bufferData( 0 )
	, _startData( 0 )
	, _bytesReceived( 0 )
{
    start();
}

~Dispatcher()
{
	if( _bufferData != 0)
	    delete _bufferData;
}

int64_t _copyFromBuffer( void * buffer, int64_t bytes )
{
    LBASSERT( _bufferData != 0 );

    uint64_t bytesRead = 0;

    if( _bytesReceived > bytes )
    {
        memcpy( buffer, _startData, bytes );
        _startData     += bytes;
        _bytesReceived -= bytes;
        bytesRead       = bytes;
    }
    else
    {
        memcpy( buffer, _startData, _bytesReceived );
        bytesRead        = _bytesReceived;
        delete _bufferData;
        _bytesReceived   = 0;
        _startData       = 0;
        _bufferData      = 0;
    }

    return bytesRead;
}

int64_t _receiveMessage( void * buffer, int64_t bytes )
{
    int64_t bytesRead = 0;

    while( bytes > 0 )
    {
        LBASSERT( _bytesReceived <= 0 );
        MPI_Status status;
        if( MPI_SUCCESS != MPI_Probe( MPI_ANY_SOURCE, _tag, MPI_COMM_WORLD, &status ) )
        {
            LBERROR << "Error retrieving messages " << std::endl;
            bytesRead  = -1;
            break;
        }

        int32_t bytesR = 0;

        /** Consult number of bytes received. */
        if( MPI_SUCCESS != MPI_Get_count( &status, MPI_BYTE, &bytesR) )
        {
            LBERROR << "Error retrieving messages " << std::endl;
            bytesRead  = -1;
            break;
        }

        if( bytesR <= bytes )
        {
            /* Receive the message, this call is not blocking due to the
             * previous MPI_Probe call.
             */
            if( MPI_SUCCESS != MPI_Recv( buffer, bytesR, MPI_BYTE, status.MPI_SOURCE,
                                            _tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE ) )
            {
                LBERROR << "Error retrieving messages " << std::endl;
                bytesRead  = -1;
                break;
            }
        
            /** If the remote has closed the connection I should get a notification. */
            if( bytesR == 1 &&
                ((unsigned char*)buffer)[0] == 0xFF )
            {
                LBINFO << "Got EOF, closing connection" << std::endl;
                bytesRead = -1;
                break;
            }

            if( status.MPI_SOURCE == _source )
            {
                bytes -= bytesR;
                buffer   = (unsigned char*)buffer + bytesR;
                bytesRead      += bytesR;
            }
            else
            {
                LBWARN << "Warning!!! Received message form wrong source" <<std::endl;
            }
        }
        else
        {
            LBASSERT( _bytesReceived == 0 );
            LBASSERT( _bufferData == 0 );
            _bufferData = new unsigned char[bytesR];
            _startData = _bufferData;

            /* Receive the message, this call is not blocking due to the
             * previous MPI_Probe call.
             */
            if( MPI_SUCCESS != MPI_Recv( _bufferData, bytesR, MPI_BYTE, status.MPI_SOURCE,
                                            _tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE ) )
            {
                LBERROR << "Error retrieving messages " << std::endl;
                bytesRead  = -1;
                break;
            }

            if( bytesR == 1 &&
                _bufferData[0] == 0xFF )
            {
                LBINFO << "Got EOF, closing connection" << std::endl;
                bytesRead = -1;
                break;
            }

            if( status.MPI_SOURCE == _source )
            {
                _bytesReceived = bytesR;

                memcpy( buffer, _startData, bytes );
                _startData     += bytes;
                bytesRead      += bytes;
                _bytesReceived -= bytes;
                bytes  = 0;
            }
            else
            {
                delete _bufferData;
                _bufferData = 0;
                _startData = 0;
                _bytesReceived = 0;

                LBWARN << "Warning!!! Received message form wrong source" <<std::endl;
            }
        }
    }

    return bytesRead;
}

void run()
{
	int64_t bytesRead = 0;
    while( 1 )
    {
        /* Wait for new petitions.
         * Warning!! MPI_Probe is is a cpu intensive
         * function.
         *
         * Note from MPI documentation:
         * It is not necessary to receive a message immediately
         * after it has been probed for, and the same message
         * may be probed for several times before it is received.
         */
        bytesRead = 0;

        if( _bytesReceived == 0 )
        {
            MPI_Status status;
            if( MPI_SUCCESS != MPI_Probe( MPI_ANY_SOURCE
                                , _tag, MPI_COMM_WORLD
                                , &status ) )
            {
                LBERROR << "Error retrieving messages " << std::endl;
                bytesRead  = -1;
                break;
            }

            _notifier->set();
        }

		Petition petition = _dispatcherQ.pop();

        /** Check if not stopped and start MPI_Probe. */
        if( petition.bytes < 0)
        {
            LBINFO << "Exit MPI dispatcher" << std::endl;
            bytesRead = -1;
            break;
        }

        /** There are bytes from last MPI_Recv */
        if( _bytesReceived > 0 )
            bytesRead = _copyFromBuffer( petition.data, petition.bytes );

        petition.bytes -= bytesRead;
        petition.data    = (unsigned char*)petition.data + bytesRead;

        int64_t ret = _receiveMessage( petition.data, petition.bytes );

        if( ret < 0 )
            bytesRead = -1;
        else
            bytesRead += ret;

        if( bytesRead < 0 )
            break;

        if( _bytesReceived == 0 )
            _notifier->reset();

		_readyQ.push( bytesRead );
    }

    LBASSERT( bytesRead < 0 )
    _readyQ.push( bytesRead );
}

int64_t readSync(void * buffer, int64_t bytes)
{
	_dispatcherQ.push( Petition{ bytes, buffer } );

    int64_t received = 0;
    if( !_readyQ.timedPop( (const unsigned) co::Global::getTimeout(), received ) )
	    return -1;

	return received;
}

bool close()
{
    /* Send remote connetion EOF and close dispatcher.
     * If is closed for unknow reason send async.
     */

	_dispatcherQ.push( Petition{ -1, 0 } );

    unsigned char eof = 0xFF;
    if( MPI_SUCCESS != MPI_Send( &eof, 1,
                            MPI_BYTE, _rank,
                            _tag, MPI_COMM_WORLD ) )
    {
        LBWARN << "Error sending eof to remote " << std::endl;
    }
    if( MPI_SUCCESS != MPI_Send( &eof, 1,
                            MPI_BYTE, _source,
                            _tagClose, MPI_COMM_WORLD ) )
    {
        LBWARN << "Error sending eof to remote " << std::endl;
    }

    join();

    /** If someone is waitting signal. */
    _notifier->set();

    return true;
}

private:

int32_t _rank;
int32_t _source;
int32_t _tag;
int32_t _tagClose;

EventConnectionPtr _notifier;

unsigned char * _bufferData;
unsigned char * _startData;
int64_t         _bytesReceived;

lunchbox::MTQueue< Petition > _dispatcherQ;
lunchbox::MTQueue< int64_t >  _readyQ;

};

}

namespace co
{

namespace detail
{

class AsyncConnection;

/** Detail for co::MPIConnection class */
class MPIConnection
{
public:

    MPIConnection() :
          rank( -1 )
        , peerRank( -1 )
        , tagSend( -1 )
        , tagRecv( -1 )
        , asyncConnection( 0 )
        , dispatcher( 0 )
        , event( new EventConnection )
    {
        // Ask rank of the process
        if( MPI_SUCCESS != MPI_Comm_rank( MPI_COMM_WORLD, &rank ) )
        {
            LBERROR << "Could not determine the rank of the calling "
                    << "process in the communicator: MPI_COMM_WORLD."
                    << std::endl;
        }

        LBASSERT( rank >= 0 );
    }

    int32_t     rank;
    int32_t     peerRank;
    int32_t     tagSend;
    int32_t     tagRecv;

    AsyncConnection * asyncConnection;
    Dispatcher  *     dispatcher;

    EventConnectionPtr  event;
};

/* Due to accept a new connection when listenting is
 * an asynchronous process, this class perform the
 * accepting process in a different thread.
 */
class AsyncConnection : lunchbox::Thread
{
public:

AsyncConnection(MPIConnection * detail, int32_t tag, EventConnectionPtr notifier) :
      _detail( detail )
    , _tag( tag )
    , _status( true )
    , _notifier( notifier )
{
    start();
}

void abort()
{
    int rank = -1;
    if( MPI_SUCCESS != MPI_Ssend( &rank, 4,
                            MPI_BYTE, _detail->rank,
                            _tag, MPI_COMM_WORLD ) )
    {
        LBWARN << "Error sending MPI tag to peer in a MPI connection." 
               << std::endl;
        return;
    }
    join();
}

bool wait()
{
    join( );
    return _status;
}

MPIConnection * getImpl()
{
    return _detail;
}

void run()
{
    MPI_Request request;

    /* Recieve the peer rank. 
     * An asychronize function is used to allow abort an 
     * acceptNB process.
     */
    if( MPI_SUCCESS != MPI_Irecv( &_detail->peerRank, 1,
                            MPI_INT, MPI_ANY_SOURCE, _tag,
                            MPI_COMM_WORLD, &request) )
    {
        LBWARN << "Could not start accepting a MPI connection, "
               << "closing connection." << std::endl;
        _status = false;
        _notifier->set();
        return;
    }

	MPI_Status status;
    if( MPI_SUCCESS !=  MPI_Wait( &request, &status ) )
    {
        LBWARN << "Could not start accepting a MPI connection, "
               << "closing connection." << std::endl;
        _status = false;
        _notifier->set();
        return;
    }

    if( _detail->peerRank < 0 )
	{
		LBINFO << "Error accepting connection from rank "
		       << _detail->peerRank << std::endl;
		_status = false;
        _notifier->set();
		return;
	}

    _detail->tagRecv = ( int32_t )tagManager.getTag( );

    // Send Tag
    if( MPI_SUCCESS != MPI_Ssend( &_detail->tagRecv, 4,
                            MPI_BYTE, _detail->peerRank,
                            _tag, MPI_COMM_WORLD ) )
    {
        LBWARN << "Error sending MPI tag to peer in a MPI connection." 
               << std::endl;
        _status = false;
        _notifier->set();
        return;
    }

    /** Receive the peer tag. */
    if( MPI_SUCCESS != MPI_Recv( &_detail->tagSend, 4,
                            MPI_BYTE, _detail->peerRank,
                            _tag, MPI_COMM_WORLD, NULL ) )
    {
        LBWARN << "Could not receive MPI tag from " 
               << _detail->peerRank << " process." << std::endl;
        _status = false;
        _notifier->set();
        return;
    }

    /** Check tag is correct. */
    LBASSERT( _detail->tagSend > 0 );

    /** Notify a new connection request */
    _notifier->set();
}

private:
MPIConnection * _detail;
int             _tag;
bool            _status;

EventConnectionPtr  _notifier;
};

}


MPIConnection::MPIConnection() :
    _impl( new detail::MPIConnection )
{
    ConnectionDescriptionPtr description = _getDescription( );
    description->type = CONNECTIONTYPE_MPI;
    description->bandwidth = 1024000; // For example :S

    LBCHECK( _impl->event->connect( ));
}

MPIConnection::MPIConnection(detail::MPIConnection * impl) :
    _impl( impl )
{
    ConnectionDescriptionPtr description = _getDescription();
    description->type = CONNECTIONTYPE_MPI;
    description->bandwidth = 1024000; // For example :S

    LBCHECK( _impl->event->connect( ));
}

MPIConnection::~MPIConnection()
{
    _close();

    if( _impl->asyncConnection != 0 )
    {
        //_impl->asyncConnection->abort();
        delete _impl->asyncConnection;
    }

    if( _impl->dispatcher != 0 )
        delete _impl->dispatcher;

    delete _impl;
}

co::Connection::Notifier MPIConnection::getNotifier() const
{
    if( isConnected() || isListening() )
        return _impl->event->getNotifier();
	else
		return -1;
}

bool MPIConnection::connect()
{
    LBASSERT( getDescription()->type == CONNECTIONTYPE_MPI );

    if( !isClosed() )
        return false;

    _setState( STATE_CONNECTING );

    ConnectionDescriptionPtr description = _getDescription( );
    _impl->peerRank = description->rank;
    int32_t cTag = description->port;

    /** To connect first send the rank. */
    if( MPI_SUCCESS != MPI_Ssend( &_impl->rank, 1,
                            MPI_INT, _impl->peerRank,
                            cTag, MPI_COMM_WORLD ) )
    {
        LBWARN << "Could not connect to "
               << _impl->peerRank << " process." << std::endl;
        _close();
        return false;
    }

    /* If the listener receive the rank, he should send
     * the MPI tag used for send.
     */
    if( MPI_SUCCESS != MPI_Recv( &_impl->tagSend, 4,
                            MPI_BYTE, _impl->peerRank,
                            cTag, MPI_COMM_WORLD, NULL ) )
    {
        LBWARN << "Could not receive MPI tag from "
               << _impl->peerRank << " process." << std::endl;
        _close();
        return false;
    }

    /** Check tag is correct. */
    LBASSERT( _impl->tagSend > 0 );

    /** Get a new tag to receive and send it. */
    _impl->tagRecv = ( int32_t )tagManager.getTag( );
    if( MPI_SUCCESS != MPI_Ssend( &_impl->tagRecv, 4,
                            MPI_BYTE, _impl->peerRank,
                            cTag, MPI_COMM_WORLD ) )
    {
        LBWARN << "Could not connect to "
               << _impl->peerRank << " process." << std::endl;
        _close();
        return false;
    }

    /** Creating the dispatcher. */
    _impl->dispatcher = new Dispatcher( _impl->rank
                                ,_impl->peerRank
                                ,_impl->tagRecv
                                ,_impl->tagSend
                                ,_impl->event );

    _setState( STATE_CONNECTED );

    LBINFO << "Connected with rank " << _impl->peerRank << " on tag "
           << _impl->tagRecv <<  std::endl;

    return true;
}

bool MPIConnection::listen()
{
    if( !isClosed())
        return false;

    int32_t tag = getDescription()->port;

    /** Register tag. */
    if( !tagManager.registerTag( tag ) )
    {
        LBWARN << "Tag " << tag << " is already register." << std::endl;
        if( isListening( ) )
            LBINFO << "Probably, listen has already called before." << std::endl;
        _close();
        return false;
    }

    /** Set tag for listening. */
    _impl->tagRecv = tag;

    LBINFO << "MPI Connection, rank " << _impl->rank 
           << " listening on tag " << _impl->tagRecv << std::endl;

    _setState( STATE_LISTENING );

    return true;
}

/* The close can occur for two reasons:
 * - Application call MPIConnection::close(), in this case
 * the remote peer should be notified by sending an EOF.
 * - Connection has been destroyed.
 */ 
void MPIConnection::_close()
{
    if( isClosed() )
        return;

    _setState( STATE_CLOSING );

    /** Close Dispacher. */
    if( _impl->dispatcher != 0 )
        _impl->dispatcher->close();

    /** Abort acceptNB */
    if( _impl->asyncConnection != 0 )
        _impl->asyncConnection->abort( );

    /** Deregister tags. */
    tagManager.deregisterTag( _impl->tagRecv );

    _impl->event->close();

    _setState( STATE_CLOSED );
}

void MPIConnection::acceptNB()
{
    LBASSERT( isListening());

    /** Ensure tag is register. */
    LBASSERT( _impl->tagRecv != -1 );

    /* Avoid multiple accepting process at the same time
     * To start a new accept process first call acceptSync
     * to finish the last one.
     */
    LBASSERT( _impl->asyncConnection == 0 )

    detail::MPIConnection * newImpl = new detail::MPIConnection( );

    _impl->asyncConnection = new detail::AsyncConnection( newImpl
                                            ,_impl->tagRecv
                                            ,_impl->event);

}

ConnectionPtr MPIConnection::acceptSync()
{
    if( !isListening( ))
        return 0;

    LBASSERT( _impl->asyncConnection != 0 )

    if( !_impl->asyncConnection->wait() )
    {
        LBWARN << "Error accepting a MPI connection, closing connection."
               << std::endl;
        _close();
        return 0;
    }

    detail::MPIConnection * newImpl = _impl->asyncConnection->getImpl( );

    delete _impl->asyncConnection;
    _impl->asyncConnection = 0;

    /** Create dispatcher of new connection. */
    newImpl->dispatcher = new Dispatcher( newImpl->rank
                                    ,newImpl->peerRank
                                    ,newImpl->tagRecv
                                    ,newImpl->tagSend
                                    ,newImpl->event );

    MPIConnection* newConnection = new MPIConnection( newImpl );
    newConnection->_setState( STATE_CONNECTED );

    LBINFO << "Accepted to rank " << newImpl->peerRank << " on tag "
           << newImpl->tagRecv << std::endl;

    _impl->event->reset();

    return newConnection;
}

int64_t MPIConnection::readSync( void* buffer, const uint64_t bytes, const bool)
{
    if( !isConnected() )
        return -1;

    const int64_t bytesRead = _impl->dispatcher->readSync( buffer, bytes );

    /** If error close. */
    if( bytesRead < 0 )
    {
        LBWARN << "Read error, closing connection" << std::endl;
        _close();
        return -1;
    }
    return bytesRead;
}

int64_t MPIConnection::write( const void* buffer, const uint64_t bytes )
{
    if( !isConnected() )
        return -1;

    if( MPI_SUCCESS != MPI_Ssend( (void*)buffer, bytes,
                            MPI_BYTE, _impl->peerRank,
                            _impl->tagSend, MPI_COMM_WORLD ) )
    {
        LBWARN << "Write error, closing connection" << std::endl;
        close();
        return -1;
    }

    return bytes;
}

}
