
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

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread/thread.hpp>

#include <set>
#include <mpi.h>

namespace
{

/*
 * Every connection inside a process has a unique MPI tag.
 * This class allows to register a MPI tag and get a new unique tag.
 *
 * Due to the tag is defined by a 16 bits integer on
 * ConnectionDescription ( the real name is port but it
 * is reused for this purpuse ). The listeners always
 * use tags [ 0, 65535 ] and others [ 65536, 2147483648 ].
 */
static class TagManager
{

public:
    TagManager()
        : _nextTag( 65536 )
    {
    }

    bool registerTag(const uint32_t tag)
    {
        lunchbox::ScopedMutex< > mutex( _lock );

        if( _tags.find( tag ) != _tags.end( ) )
            return false;

        _tags.insert( tag );

        return true;
    }

    void deregisterTag(const uint32_t tag)
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

}

namespace co
{

MPIConnection::Dispatcher::Dispatcher( const int32_t rank,
                                        const int32_t source,
                                        const int32_t tag,
                                        const int32_t tagClose,
                                        EventConnectionPtr notifier)
        :  _rank( rank )
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

MPIConnection::Dispatcher::~Dispatcher()
{
    delete _bufferData;
}

int64_t MPIConnection::Dispatcher::_copyFromBuffer( void * buffer,
                                                        const int64_t bytes )
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

int64_t MPIConnection::Dispatcher::_receiveMessage( void * buffer,
                                                        int64_t bytes )
{
    int64_t bytesRead = 0;

    while( bytes > 0 )
    {
        LBASSERT( _bytesReceived <= 0 );
        MPI_Status status;
        if( MPI_SUCCESS != MPI_Probe( MPI_ANY_SOURCE,
                                        _tag,
                                        MPI_COMM_WORLD,
                                        &status ) )
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
            if( MPI_SUCCESS != MPI_Recv( buffer, bytesR, MPI_BYTE,
                                         status.MPI_SOURCE,
                                         _tag,
                                         MPI_COMM_WORLD,
                                         MPI_STATUS_IGNORE ) )
            {
                LBERROR << "Error retrieving messages " << std::endl;
                bytesRead  = -1;
                break;
            }

            /* If the remote has closed the connection I should get
             * a notification.
             */
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
                LBWARN << "Warning!!! Received message form wrong source"
                       << std::endl;
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
            if( MPI_SUCCESS != MPI_Recv( _bufferData, bytesR, MPI_BYTE,
                                            status.MPI_SOURCE,
                                            _tag,
                                            MPI_COMM_WORLD,
                                            MPI_STATUS_IGNORE ) )
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

                LBWARN << "Warning!!! Received message form "
                       << "wrong source" <<std::endl;
            }
        }
    }

    return bytesRead;
}

void MPIConnection::Dispatcher::run()
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

        /** Waiting for new data if there is not available. */
        if( _bytesReceived == 0 )
        {
            MPI_Status status;
            if( MPI_SUCCESS != MPI_Probe( MPI_ANY_SOURCE,
                                            _tag,
                                            MPI_COMM_WORLD,
                                            &status ) )
            {
                LBERROR << "Error retrieving messages " << std::endl;
                bytesRead  = -1;
                break;
            }

            _notifier->set();
        }

        /** Wait for petition, the push is performed in readSync. */
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

        /** Notify the petition has been finished. */
        _readyQ.push( bytesRead );
    }

    LBASSERT( bytesRead < 0 )
    _readyQ.push( bytesRead );
}

int64_t MPIConnection::Dispatcher::readSync(void * buffer, const int64_t bytes)
{
    _dispatcherQ.push( Petition{ bytes, buffer } );

    int64_t received = 0;
    if( !_readyQ.timedPop(  (const unsigned) co::Global::getTimeout()
                            , received ) )
        return -1;

    return received;
}

bool MPIConnection::Dispatcher::close()
{
    /* Send remote connetion EOF and close dispatcher.
     * If is closed for unknow reason send async.
     */

    _dispatcherQ.push( Petition{ -1, 0 } );

    unsigned char eof = 0xFF;
    if( MPI_SUCCESS != MPI_Send( &eof, 1,
                                    MPI_BYTE,
                                    _rank,
                                    _tag,
                                    MPI_COMM_WORLD ) )
    {
        LBWARN << "Error sending eof to remote " << std::endl;
    }
    if( MPI_SUCCESS != MPI_Send( &eof, 1,
                                    MPI_BYTE,
                                    _source,
                                    _tagClose,
                                    MPI_COMM_WORLD ) )
    {
        LBWARN << "Error sending eof to remote " << std::endl;
    }

    join();

    /** If someone is waitting signal. */
    _notifier->set();

    return true;
}

MPIConnection::AsyncConnection::AsyncConnection( MPIConnection * detail,
                                                    const int32_t tag,
                                                    EventConnectionPtr notifier)
    : _detail( detail )
    , _tag( tag )
    , _status( true )
    , _notifier( notifier )
{
    start();
}

void MPIConnection::AsyncConnection::abort()
{
    /** Send a no rank to wake the thread up. */
    int rank = -1;
    if( MPI_SUCCESS != MPI_Ssend( &rank, 4,
                                    MPI_BYTE,
                                    _detail->getRank(),
                                    _tag,
                                    MPI_COMM_WORLD ) )
    {
        LBWARN << "Error sending MPI tag to peer in a MPI connection."
               << std::endl;
        return;
    }
    join();
}

bool MPIConnection::AsyncConnection::wait()
{
    join( );
    return _status;
}

MPIConnection * MPIConnection::AsyncConnection::getImpl()
{
    return _detail;
}

void MPIConnection::AsyncConnection::run()
{
    MPI_Request request;

    int32_t peerRank = -1;
    /* Recieve the peer rank.
     * An asychronize function is used to allow future
     * sleep and wait due to save cpu.
     */
    if( MPI_SUCCESS != MPI_Irecv( &peerRank, 1,
                                    MPI_INT,
                                    MPI_ANY_SOURCE,
                                    _tag,
                                    MPI_COMM_WORLD,
                                    &request ) )
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

    if( peerRank < 0 )
    {
        LBINFO << "Error accepting connection from rank "
               << peerRank << std::endl;
        _status = false;
        _notifier->set();
        return;
    }

    _detail->setPeerRank( peerRank );

    int32_t tR = ( int32_t )tagManager.getTag( );
    _detail->setTagRecv( tR );

    // Send Tag
    if( MPI_SUCCESS != MPI_Ssend( &tR, 4,
                                    MPI_BYTE,
                                    peerRank,
                                    _tag,
                                    MPI_COMM_WORLD ) )
    {
        LBWARN << "Error sending MPI tag to peer in a MPI connection."
               << std::endl;
        _status = false;
        _notifier->set();
        return;
    }

    int32_t tS = -1;

    /** Receive the peer tag. */
    if( MPI_SUCCESS != MPI_Recv( &tS, 4,
                                    MPI_BYTE,
                                    peerRank,
                                    _tag,
                                    MPI_COMM_WORLD,
                                    NULL ) )
    {
        LBWARN << "Could not receive MPI tag from "
               << peerRank << " process." << std::endl;
        _status = false;
        _notifier->set();
        return;
    }

    /** Check tag is correct. */
    LBASSERT( tS > 0 );
    _detail->setTagSend( tS );

    /** Notify a new connection request */
    _notifier->set();
}

MPIConnection::MPIConnection()
        : _rank( -1 )
        , _peerRank( -1 )
        , _tagSend( -1 )
        , _tagRecv( -1 )
        , _asyncConnection( 0 )
        , _dispatcher( 0 )
        , _event( new EventConnection )
{
    // Ask rank of the process
    lunchbox::MPI mpi;
    _rank = mpi.getRank();

    LBASSERT( _rank >= 0 );
    ConnectionDescriptionPtr description = _getDescription( );
    description->type = CONNECTIONTYPE_MPI;
    description->bandwidth = 1024000; // For example :S

    LBCHECK( _event->connect( ));
}

MPIConnection::~MPIConnection()
{
    _close();

    delete _asyncConnection;
    delete _dispatcher;
}

void MPIConnection::startDispatcher()
{
    /** Creating the dispatcher. */
    if( _dispatcher == 0 )
        _dispatcher = new Dispatcher( _rank,
                                        _peerRank,
                                        _tagRecv,
                                        _tagSend,
                                        _event );
}

co::Connection::Notifier MPIConnection::getNotifier() const
{
    if( isConnected() || isListening() )
        return _event->getNotifier();

    return -1;
}

bool MPIConnection::connect()
{
    LBASSERT( getDescription()->type == CONNECTIONTYPE_MPI );

    if( !isClosed() )
        return false;

    _setState( STATE_CONNECTING );

    ConnectionDescriptionPtr description = _getDescription( );
    _peerRank = description->rank;
    const int32_t cTag = description->port;

    /** To connect first send the rank. */
    if( MPI_SUCCESS != MPI_Ssend( &_rank, 1,
                                    MPI_INT,
                                    _peerRank,
                                    cTag,
                                    MPI_COMM_WORLD ) )
    {
        LBWARN << "Could not connect to "
               << _peerRank << " process." << std::endl;
        _close();
        return false;
    }

    /* If the listener receive the rank, he should send
     * the MPI tag used for send.
     */
    if( MPI_SUCCESS != MPI_Recv( &_tagSend, 4,
                                    MPI_BYTE,
                                    _peerRank,
                                    cTag,
                                    MPI_COMM_WORLD, NULL ) )
    {
        LBWARN << "Could not receive MPI tag from "
               << _peerRank << " process." << std::endl;
        _close();
        return false;
    }

    /** Check tag is correct. */
    LBASSERT( _tagSend > 0 );

    /** Get a new tag to receive and send it. */
    _tagRecv = ( int32_t )tagManager.getTag( );

    if( MPI_SUCCESS != MPI_Ssend( &_tagRecv, 4,
                                    MPI_BYTE,
                                    _peerRank,
                                    cTag,
                                    MPI_COMM_WORLD ) )
    {
        LBWARN << "Could not connect to "
               << _peerRank << " process." << std::endl;
        _close();
        return false;
    }

    /** Start the dispatcher. */
    startDispatcher();

    _setState( STATE_CONNECTED );

    LBINFO << "Connected with rank " << _peerRank << " on tag "
           << _tagRecv <<  std::endl;

    return true;
}

bool MPIConnection::listen()
{
    if( !isClosed())
        return false;

    const int32_t tag = getDescription()->port;

    /** Register tag. */
    if( !tagManager.registerTag( tag ) )
    {
        LBWARN << "Tag " << tag << " is already register." << std::endl;
        if( isListening( ) )
            LBINFO << "Probably, listen has already called before."
                   << std::endl;
        _close();
        return false;
    }

    /** Set tag for listening. */
    _tagRecv = tag;

    LBINFO << "MPI Connection, rank " << _rank
           << " listening on tag " << _tagRecv << std::endl;

    _setState( STATE_LISTENING );

    return true;
}

void MPIConnection::_close()
{
    if( isClosed() )
        return;

    _setState( STATE_CLOSING );

    /** Close Dispacher. */
    if( _dispatcher != 0 )
        _dispatcher->close();

    /** Abort acceptNB */
    if( _asyncConnection != 0 )
        _asyncConnection->abort( );

    /** Deregister tags. */
    tagManager.deregisterTag( _tagRecv );

    _event->close();

    _setState( STATE_CLOSED );
}

void MPIConnection::acceptNB()
{
    LBASSERT( isListening());

    /** Ensure tag is register. */
    LBASSERT( _tagRecv != -1 );

    /* Avoid multiple accepting process at the same time
     * To start a new accept process first call acceptSync
     * to finish the last one.
     */
    LBASSERT( _asyncConnection == 0 )

    MPIConnection * newConn = new MPIConnection( );

    _asyncConnection = new AsyncConnection( newConn,
                                             _tagRecv,
                                             _event);
}

ConnectionPtr MPIConnection::acceptSync()
{
    if( !isListening( ))
        return 0;

    LBASSERT( _asyncConnection != 0 )

        if( !_asyncConnection->wait() )
        {
            LBWARN << "Error accepting a MPI connection, closing connection."
                   << std::endl;
            _close();
            return 0;
        }

    MPIConnection * newConn = _asyncConnection->getImpl( );

    delete _asyncConnection;
    _asyncConnection = 0;

    /** Start dispatcher of new connection. */
    newConn->startDispatcher();

    newConn->_setState( STATE_CONNECTED );

    LBINFO << "Accepted to rank " << newConn->getPeerRank() << " on tag "
           << newConn->getTagRecv() << std::endl;

    _event->reset();

    return newConn;
}

int64_t MPIConnection::readSync( void* buffer, const uint64_t bytes, const bool)
{
    if( !isConnected() )
        return -1;

    const int64_t bytesRead = _dispatcher->readSync( buffer, bytes );

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
                                    MPI_BYTE,
                                    _peerRank,
                                    _tagSend,
                                    MPI_COMM_WORLD ) )
    {
        LBWARN << "Write error, closing connection" << std::endl;
        close();
        return -1;
    }

    return bytes;
}

}
