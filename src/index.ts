import { Worker } from "@notionhq/workers";
import * as Schema from "@notionhq/workers/schema";
import * as Builder from "@notionhq/workers/builder";

const worker = new Worker();
export default worker;

// --- OAuth ---

const googleAuth = worker.oauth("googleAuth", {
	name: "google-analytics-oauth",
	authorizationEndpoint: "https://accounts.google.com/o/oauth2/v2/auth",
	tokenEndpoint: "https://oauth2.googleapis.com/token",
	scope: "https://www.googleapis.com/auth/analytics.readonly",
	clientId: process.env.GOOGLE_CLIENT_ID ?? "",
	clientSecret: process.env.GOOGLE_CLIENT_SECRET ?? "",
	authorizationParams: {
		access_type: "offline",
		prompt: "consent",
	},
});

// --- Database ---

const pagesDb = worker.database("pagesPathReportDb", {
	type: "managed",
	initialTitle: "Pages Path Report",
	primaryKeyProperty: "Name",
	schema: {
		databaseIcon: Builder.emojiIcon("📄"),
		properties: {
			"Name": Schema.title(),
			"Date": Schema.date("YYYY/MM/DD"),
			"Page Path": Schema.richText(),
			"Screen Page Views": Schema.number(),
			"New Users": Schema.number(),
			"Total Users": Schema.number(),
			"User Engagement Duration": Schema.number(),
		},
	},
});

// --- GA4 API helper ---

interface GA4Row {
	date: string;
	pagePath: string;
	screenPageViews: number;
	newUsers: number;
	totalUsers: number;
	userEngagementDuration: number;
}

interface GA4Response {
	rows: GA4Row[];
	totalRows: number;
}

async function fetchGA4Report(
	token: string,
	propertyId: string,
	startDate: string,
	endDate: string,
	limit: number,
	offset: number,
): Promise<GA4Response> {
	const url = `https://analyticsdata.googleapis.com/v1beta/properties/${propertyId}:runReport`;

	const response = await fetch(url, {
		method: "POST",
		headers: {
			Authorization: `Bearer ${token}`,
			"Content-Type": "application/json",
		},
		body: JSON.stringify({
			dateRanges: [{ startDate, endDate }],
			dimensions: [{ name: "date" }, { name: "pagePath" }],
			metrics: [
				{ name: "screenPageViews" },
				{ name: "newUsers" },
				{ name: "totalUsers" },
				{ name: "userEngagementDuration" },
			],
			limit,
			offset,
		}),
	});

	if (!response.ok) {
		const body = await response.text();
		throw new Error(`GA4 API error ${response.status}: ${body}`);
	}

	const data = (await response.json()) as {
		rows?: Array<{
			dimensionValues: Array<{ value: string }>;
			metricValues: Array<{ value: string }>;
		}>;
		rowCount?: number;
	};

	const rows: GA4Row[] = (data.rows ?? []).map((row) => ({
		date: row.dimensionValues[0].value,
		pagePath: row.dimensionValues[1].value,
		screenPageViews: parseInt(row.metricValues[0].value, 10),
		newUsers: parseInt(row.metricValues[1].value, 10),
		totalUsers: parseInt(row.metricValues[2].value, 10),
		userEngagementDuration: parseFloat(row.metricValues[3].value),
	}));

	return { rows, totalRows: data.rowCount ?? 0 };
}

function formatGA4Date(yyyymmdd: string): string {
	return `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}`;
}

function getDateNDaysAgo(n: number): string {
	const d = new Date();
	d.setDate(d.getDate() - n);
	return d.toISOString().slice(0, 10);
}

// --- Sync ---

const BATCH_SIZE = 250;
const LOOKBACK_DAYS = 30;

interface SyncState {
	offset: number;
}

worker.sync("pagesPathReport", {
	database: pagesDb,
	schedule: "1h",
	mode: "replace",
	execute: async (state: SyncState | undefined) => {
		const propertyId = process.env.GA4_PROPERTY_ID;
		if (!propertyId) {
			throw new Error("GA4_PROPERTY_ID secret is not set");
		}

		const token = await googleAuth.accessToken();
		const offset = state?.offset ?? 0;
		const startDate = getDateNDaysAgo(LOOKBACK_DAYS);
		const endDate = getDateNDaysAgo(1);

		const { rows, totalRows } = await fetchGA4Report(
			token,
			propertyId,
			startDate,
			endDate,
			BATCH_SIZE,
			offset,
		);

		const changes = rows.map((row) => ({
			type: "upsert" as const,
			key: `${row.date}::${row.pagePath}`,
			properties: {
				"Name": Builder.title(row.pagePath),
				"Date": Builder.date(formatGA4Date(row.date)),
				"Page Path": Builder.richText(row.pagePath),
				"Screen Page Views": Builder.number(row.screenPageViews),
				"New Users": Builder.number(row.newUsers),
				"Total Users": Builder.number(row.totalUsers),
				"User Engagement Duration": Builder.number(row.userEngagementDuration),
			},
		}));

		const nextOffset = offset + rows.length;
		const hasMore = nextOffset < totalRows;

		return {
			changes,
			hasMore,
			nextState: hasMore ? { offset: nextOffset } : undefined,
		};
	},
});
