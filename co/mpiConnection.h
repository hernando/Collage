
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

#ifndef CO_MPICONNECTION_H
#define CO_MPICONNECTION_H

#include <co/connection.h>
#include <co/eventConnection.h>

#include <lunchbox/monitor.h>
#include <lunchbox/scopedMutex.h>
#include <lunchbox/mtQueue.h>
#include <lunchbox/thread.h>
#include<lunchbox/mpi.h>

#include <map>

namespace co
{

namespace
{

typedef lunchbox::RefPtr< co::EventConnection > EventConnectionPtr;

}

/* MPI connection
 * Allows peer-to-peer connections if the MPI runtime environment has
 * been set correctly.
 *
 * Due to Collage is a multithreaded library, MPI connections
 * requiere at least MPI_THREAD_SERIALIZED level of thread support.
 * During the initialization Collage will request the appropriate
 * thread support but if the MPI library does not provide it, MPI
 * connections will be disabled. If the application uses a MPI
 * connection when disabled, the connection should not be created.
 */
class MPIConnection : public Connection
{
    public:
        /** Construct a new MPI connection. */
        CO_API MPIConnection();

        /** Destruct this MPI connection. */
        CO_API ~MPIConnection();

        virtual bool connect();
        virtual bool listen();
        virtual void close() { _close(); }

        virtual void acceptNB();
        virtual ConnectionPtr acceptSync();

        virtual Notifier getNotifier() const;

        void setPeerRank( int32_t peerRank ) { _peerRank = peerRank; }
        void setTagSend( int32_t tagSend ) { _tagSend = tagSend; }
        void setTagRecv( int32_t tagRecv ) { _tagRecv = tagRecv; }
        void startDispatcher();
        int32_t getRank() { return _rank; }
        int32_t getPeerRank() { return _peerRank; }
        int32_t getTagSend() { return _tagSend; }
        int32_t getTagRecv() { return _tagRecv; }

    protected:
        void readNB( void* , const uint64_t ) { /* NOP */ }
        int64_t readSync( void* buffer, const uint64_t bytes,
                          const bool ignored);
        int64_t write( const void* buffer, const uint64_t bytes );

    private:
        void _close();

        struct Petition
        {
            int64_t bytes;
            void *  data;
        };

        class Dispatcher : lunchbox::Thread
        {
        public:
            Dispatcher( const int32_t rank, const int32_t source,
                        const int32_t tag, const int32_t tagClose,
                        EventConnectionPtr notifier);

            ~Dispatcher();

            virtual void run();

            int64_t readSync(void * buffer, const int64_t bytes);

            bool close();
        private:
            int64_t _copyFromBuffer( void * buffer, const int64_t bytes );

            int64_t _receiveMessage( void * buffer, int64_t bytes );

            const int32_t _rank;
            const int32_t _source;
            const int32_t _tag;
            const int32_t _tagClose;

            EventConnectionPtr _notifier;

            unsigned char * _bufferData;
            unsigned char * _startData;
            int64_t         _bytesReceived;

            lunchbox::MTQueue< Petition > _dispatcherQ;
            lunchbox::MTQueue< int64_t >  _readyQ;

        };

        /* Due to accept a new connection when listenting is
         * an asynchronous process, this class perform the
         * accepting process in a different thread.
         */
        class AsyncConnection : lunchbox::Thread
        {
        public:
            AsyncConnection( MPIConnection * detail, const int32_t tag,
                                EventConnectionPtr notifier);
            void abort();

            bool wait();

            MPIConnection * getImpl();

            virtual void run();
        private:
            MPIConnection * _detail;
            const int32_t   _tag;
            bool            _status;

            EventConnectionPtr  _notifier;
        };

        int32_t     _rank;
        int32_t     _peerRank;
        int32_t     _tagSend;
        int32_t     _tagRecv;

        AsyncConnection * _asyncConnection;
        Dispatcher  *     _dispatcher;

        EventConnectionPtr  _event;
};

}
#endif //CO_MPICONNECTION_H
