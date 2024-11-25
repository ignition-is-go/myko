// @ts-ignore
import { dev } from '$app/environment';

// @ts-ignore
import { env } from '$env/dynamic/public';
import * as WSM from '@myko/ws';

const connectHandlers: ((str: string) => void)[] = [];
const disconnectHandlers: (() => void)[] = [];
const onStartConnectHandlers: (() => void)[] = [];

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
		secure: !dev,
		disableMsgPack: !!dev,
		preventThrowing: true
	}
);

client.connect(env.PUBLIC_SERVER_ADDRESS ?? window.location.hostname, WSM.MYKO_WS_PORT);

export const onStartConnect = (func: () => void) => {
	onStartConnectHandlers.push(func);
};

export const onConnect = (func: (url: string) => void) => {
	connectHandlers.push(func);
};

export const onDisconnect = (func: () => void) => {
	disconnectHandlers.push(func);
};
