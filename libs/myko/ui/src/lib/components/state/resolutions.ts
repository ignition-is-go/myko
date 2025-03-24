import { Duration } from 'luxon';

export type Resolution = {
	milis: number;
	majorFormat: string;
	minorFormat: string;
};

export const resolutions = [
	{
		label: '1ms',
		majorFormat: 'dd MMM HH:mm:ss.SSS',
		minorFormat: '.SSS',
		milis: Duration.fromObject({ milliseconds: 1 }).as('milliseconds')
	},
	{
		label: '5ms',
		majorFormat: 'dd MMM HH:mm:ss.SSS',
		minorFormat: '.SSS',
		milis: Duration.fromObject({ milliseconds: 5 }).as('milliseconds')
	},
	{
		label: '10ms',
		minorFormat: '.SSS',
		majorFormat: 'dd MMM HH:mm:ss.SSS',
		milis: Duration.fromObject({ milliseconds: 10 }).as('milliseconds')
	},
	{
		label: '50ms',
		minorFormat: '.SSS',
		majorFormat: 'dd MMM HH:mm:ss.SSS',
		milis: Duration.fromObject({ milliseconds: 50 }).as('milliseconds')
	},
	{
		label: '100ms',
		majorFormat: 'dd MMM HH:mm:ss.SSS',
		minorFormat: '.SSS',

		milis: Duration.fromObject({ milliseconds: 100 }).as('milliseconds')
	},
	{
		label: '500ms',
		majorFormat: 'dd MMM HH:mm:ss.SSS',
		minorFormat: '.SSS',
		milis: Duration.fromObject({ milliseconds: 500 }).as('milliseconds')
	},
	{
		label: '1s',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'ss',
		milis: Duration.fromObject({ seconds: 1 }).as('milliseconds')
	},
	{
		label: '5s',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'ss',
		milis: Duration.fromObject({ seconds: 5 }).as('milliseconds')
	},
	{
		label: '10s',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'ss',
		milis: Duration.fromObject({ seconds: 10 }).as('milliseconds')
	},
	{
		label: '30s',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'ss',
		milis: Duration.fromObject({ seconds: 30 }).as('milliseconds')
	},
	{
		label: '1m',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'mm:ss',
		milis: Duration.fromObject({ minutes: 1 }).as('milliseconds')
	},
	{
		label: '5m',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'mm:ss',
		milis: Duration.fromObject({ minutes: 5 }).as('milliseconds')
	},
	{
		label: '10m',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'mm:ss',
		milis: Duration.fromObject({ minutes: 10 }).as('milliseconds')
	},
	{
		label: '30m',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'mm:ss',
		milis: Duration.fromObject({ minutes: 30 }).as('milliseconds')
	},
	{
		label: '1h',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'HH:mm',
		milis: Duration.fromObject({ hours: 1 }).as('milliseconds')
	},
	{
		label: '6h',
		majorFormat: 'dd MMM HH:mm:ss',
		minorFormat: 'HH:mm',
		milis: Duration.fromObject({ hours: 6 }).as('milliseconds')
	},
	{
		label: '12h',
		majorFormat: 'dd MMM',
		minorFormat: 'HH:mm',
		milis: Duration.fromObject({ hours: 12 }).as('milliseconds')
	},
	{
		label: '1d',
		majorFormat: 'dd MMM',
		minorFormat: 'dd',
		milis: Duration.fromObject({ days: 1 }).as('milliseconds')
	},
	{
		label: '1w',
		majorFormat: 'dd MMM yyyy',
		minorFormat: 'dd',
		milis: Duration.fromObject({ weeks: 1 }).as('milliseconds')
	},
	{
		label: '1M',
		majorFormat: 'MMM yyyy',
		minorFormat: 'MMM',
		milis: Duration.fromObject({ months: 1 }).as('milliseconds')
	},
	{
		label: '1y',
		majorFormat: 'yyyy',
		minorFormat: 'yyyy',
		milis: Duration.fromObject({ years: 1 }).as('milliseconds')
	}
];
