import * as WSM from '@myko/ws';
import { env } from '$env/dynamic/public';

const connectHandlers: ((str: string) => void)[] = [];
const disconnectHandlers: (() => void)[] = [];
const onStartConnectHandlers: (() => void)[] = [];

const isHttps = window?.location.protocol === 'https:';

// Check if environment explicitly sets secure WebSocket preference
// Default: Use secure WebSocket if page is HTTPS, or if explicitly enabled in env
const shouldUseSecure =
	env.PUBLIC_USE_SECURE_WEBSOCKET !== undefined
		? env.PUBLIC_USE_SECURE_WEBSOCKET === 'true'
		: isHttps;

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
		secure: shouldUseSecure,
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
