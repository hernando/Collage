
/* Copyright (c) 2005-2014, Carlos Duelo <cduelo@cesvima.upm.es>
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

#include <boost/interprocess/ipc/message_queue.hpp>

namespace co
{

namespace detail
{
	class AsyncConnection;

	class MPIConnection
	{
		public:
			MPIConnection() : 
							_rank(-1),
							_peerRank(-1),
							_readQ(0),
							_writeQ(0),
							_asyncConnection(0),
							_threadComm(false)
			{
				// Ask rank of the process
				if (MPI_SUCCESS != MPI_Comm_rank(MPI_COMM_WORLD, &_rank))
				{
					LBERROR << "Could not determine the rank of the calling process in the communicator: MPI_COMM_WORLD" << std::endl;
				}

				LBASSERT( _rank >= 0 );
			}

			int			_rank;
			int			_peerRank;

			// InterProcessor communication 
			std::map<void *, MPI_Request>	_requests;
			MPI_Comm						_interComm;

			// InterThread communication
			boost::interprocess::message_queue * _readQ;
			boost::interprocess::message_queue * _writeQ;
			
			AsyncConnection * _asyncConnection;
			
			bool _threadComm;
	};

	class AsyncConnection : lunchbox::Thread
	{
		public:

			AsyncConnection(MPIConnection * detail, MPI_Request * request) : 
																	_detail(detail),
																	_status(false),
																	_request(request)
			{
				start();
			}

			bool wait()
			{
				join();
				return _status;
			}

			MPIConnection * getImpl()
			{
				return _detail;
			}

			void run()
			{
				if (MPI_SUCCESS != MPI_Wait(_request, NULL))
				{
					LBERROR << "Error waiting for peer rank in a MPI connection."<< std::endl;
					_status = false;
					return;
				}
				delete _request;

				LBASSERT( _detail->_peerRank >= 0 );

				_detail->_threadComm = _detail->_rank == _detail->_peerRank;

				if (_detail->_threadComm)
				{
				}
				else
				{
					char mpiPort[MPI_MAX_PORT_NAME];

					if (MPI_SUCCESS != MPI_Open_port(MPI_INFO_NULL, mpiPort))
					{
						LBERROR << "Error openning a port in a MPI connection."<< std::endl;
						_status = false;
						return;
					}

					if (MPI_SUCCESS != MPI_Send(mpiPort, MPI_MAX_PORT_NAME, MPI_CHAR, _detail->_peerRank, 0, MPI_COMM_WORLD))
					{
						LBERROR << "Error sending name port to peer in a MPI connection."<< std::endl;
						_status = false;
						return;
					}

					if ( MPI_SUCCESS != MPI_Comm_accept(mpiPort, MPI_INFO_NULL, _detail->_rank, MPI_COMM_SELF, &_detail->_interComm))
					{
						LBERROR << "Error accepting peer in a MPI connection."<< std::endl;
						_status = false;
						return;
					}

					if (MPI_SUCCESS != MPI_Close_port(mpiPort))
					{
						LBERROR << "Error closing a port in aMPI connection."<< std::endl;
						_status = false;
						return;
					}
				}

				_status = true;
			}

		private:
			MPIConnection * _detail;

			bool		_status;

			MPI_Request * _request;
	};

}


MPIConnection::MPIConnection() : 
					_notifier(-1),
					_impl(new detail::MPIConnection)
{
	ConnectionDescriptionPtr description = _getDescription();
	description->type = CONNECTIONTYPE_MPI;
    description->bandwidth = 1024000; // For example :S
}

MPIConnection::MPIConnection(detail::MPIConnection * impl) : 
					_notifier(-1),
					_impl(impl)
{
	ConnectionDescriptionPtr description = _getDescription();
	description->type = CONNECTIONTYPE_MPI;
    description->bandwidth = 1024000; // For example :S
}

MPIConnection::~MPIConnection()
{
	if (_impl->_asyncConnection!= 0)
		delete _impl->_asyncConnection;
	
	if (_impl->_readQ != 0 && _impl->_readQ->get_num_msg() > 0)
		LBINFO << "Communication closed with pending messages"<<std::endl;
	
	if (_impl->_readQ != 0)
		delete _impl->_readQ;

	if (_impl->_writeQ != 0)
		delete _impl->_writeQ;

	close();

	delete _impl;
}

bool MPIConnection::connect()
{
    LBASSERT( getDescription()->type == CONNECTIONTYPE_MPI );

    if( !isClosed() )
        return false;

    _setState( STATE_CONNECTING );

	ConnectionDescriptionPtr description = _getDescription();
	_impl->_peerRank = description->port;

	// Same ranks then thread comminication
	_impl->_threadComm = _impl->_rank == _impl->_peerRank;

	if (MPI_SUCCESS != MPI_Send(&_impl->_rank, 1, MPI_INT, _impl->_peerRank, 0, MPI_COMM_WORLD))
	{
		LBERROR << "Could not connect to "<< _impl->_peerRank << " process."<< std::endl;
		return false;
	}

	if (_impl->_threadComm)
	{
	}
	else
	{
		char mpiPort[MPI_MAX_PORT_NAME];

		if (MPI_SUCCESS != MPI_Recv(mpiPort, MPI_MAX_PORT_NAME, MPI_CHAR, _impl->_peerRank, 0, MPI_COMM_WORLD, NULL))
		{
			LBERROR << "Could not receive name port from "<< _impl->_peerRank << " process."<< std::endl;
			return false;
		}

		if (MPI_SUCCESS != MPI_Comm_connect(mpiPort, MPI_INFO_NULL, _impl->_peerRank, MPI_COMM_SELF, &_impl->_interComm))
		{
			LBERROR << "Could not connect to "<< _impl->_peerRank << " process."<< std::endl;
			return false;
		}
	}

    _setState( STATE_CONNECTED );

    LBINFO << "Connected " << description->toString() << std::endl;

	return true;
}

bool MPIConnection::listen()
{
    if( !isClosed( ))
        return false;

    _setState( STATE_LISTENING );

	return true;
}

void MPIConnection::close()
{
    if( isClosed( ))
        return;

	if (!isListening() && !_impl->_threadComm)
		MPI_Comm_disconnect(&_impl->_interComm);

    _setState( STATE_CLOSED );
}

void MPIConnection::acceptNB()
{
	// Avoid multiple accepting process at the same time
	// To start a new accept proccess first call acceptSync to finish the last one.
	LBASSERT( _impl->_asyncConnection == 0 )

	MPI_Request * request = new MPI_Request;

	detail::MPIConnection * newImpl = new detail::MPIConnection();

	// Recieve the peer rank
	if (MPI_SUCCESS != MPI_Irecv(&newImpl->_peerRank, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, request))
	{
		LBERROR << "Could not start accepting a MPI connection."<< std::endl;
		close();
		return;
	}

	_impl->_asyncConnection = new detail::AsyncConnection(newImpl, request);
}

ConnectionPtr MPIConnection::acceptSync()
{
    if( !isListening( ))
        return 0;
	
	LBASSERT(_impl->_asyncConnection != 0)

	if (!_impl->_asyncConnection->wait())
	{
		LBERROR << "Error accepting a MPI connection."<< std::endl;
		close();
		return 0;
	}
	
	detail::MPIConnection * newImpl = _impl->_asyncConnection->getImpl();
	
	delete _impl->_asyncConnection;
	_impl->_asyncConnection = 0;

    MPIConnection* newConnection = new MPIConnection(newImpl);
    newConnection->_setState( STATE_CONNECTED );

    LBINFO << "Accepted " << getDescription()->toString() << std::endl;

	return newConnection;
}

void MPIConnection::readNB( void* buffer, const uint64_t bytes )
{
    if( isClosed() )
        return;

	if (_impl->_threadComm)
	{
	}
	else
	{
		_impl->_requests.insert(std::pair<void*,MPI_Request>(buffer, MPI_Request {}));
		MPI_Request * request = &(_impl->_requests.find(buffer)->second);

		if (MPI_SUCCESS != MPI_Irecv(buffer, bytes, MPI_BYTE, 0, 0, _impl->_interComm, request))
		{
			LBWARN << "Read error" << lunchbox::sysError << std::endl;
			close();
		}
	}
}

int64_t MPIConnection::readSync( void* buffer, const uint64_t bytes, const bool ignored)
{
	if (ignored){}
    if( !isConnected())
        return -1;

	if (_impl->_threadComm)
	{
	}
	else
	{
		std::map<void*,MPI_Request>::iterator it = _impl->_requests.find(buffer);
		LBASSERT( it != _impl->_requests.end() )
		MPI_Request * request = &(it->second);

		if (MPI_SUCCESS != MPI_Wait(request, NULL))
		{
			LBWARN << "Read error" << lunchbox::sysError << std::endl;
			close();
			return -1;
		}

		_impl->_requests.erase(it);
	}

	return bytes;
}

int64_t MPIConnection::write( const void* buffer, const uint64_t bytes )
{
    if( !isConnected())
        return -1;
		
	if (_impl->_threadComm)
	{
	}
	else
	{
		if (MPI_SUCCESS != MPI_Send((void*)buffer, bytes, MPI_BYTE, 0, 0, _impl->_interComm)) 
		{
			LBWARN << "Write error" << lunchbox::sysError << std::endl;
			close();
			return -1;
		}
	}

	return bytes;
}

}
