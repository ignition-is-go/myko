import * as WSM from '@myko/ws';

const connectHandlers: ((str: string) => void)[] = [];
const disconnectHandlers: (() => void)[] = [];
const onStartConnectHandlers: (() => void)[] = [];

const isHttps = window?.location.protocol === 'https:';
// Force secure WebSocket for remote servers (nyc.rship.io)
const isRemoteServer = window?.location.hostname === 'localhost' || window?.location.hostname === '127.0.0.1';

export const client = new WSM.WSMClient(
	(url) => {
		return new WebSocket(url);
	},
	{
		onLog: (...l) => console.log(...l),
		onError: (...l) => console.error(...l),
		onServerConnect: (url) => {
			connectHandlers.forEach((h) => h(url));
		},
		onStartConnect: () => {
			onStartConnectHandlers.forEach((h) => h());
		},
		onTerminated: () => {
			disconnectHandlers.forEach((h) => h());
		}
	},
	{
		secure: isHttps || isRemoteServer, // Use wss:// for remote connections from localhost
		disableMsgPack: true,
		preventThrowing: true
	}
);

export const onStartConnect = (func: () => void) => {
	onStartConnectHandlers.push(func);
};

export const onConnect = (func: (url: string) => void) => {
	connectHandlers.push(func);
};

export const onDisconnect = (func: () => void) => {
	disconnectHandlers.push(func);
};
