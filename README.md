### Table of Contents

- [Entities](#entities)
- [Queries](#queries)
- [Commands](#commands)

# Entities

## Action

An action to be sent to a target
### Queries

- [GetActions](#getactions)
- [GetActionsByIds](#getactionsbyids)
- [GetActionsByQuery](#getactionsbyquery)

```ts
{
	// Searchable
	name: string
	
	schema: JSONSchema
	
	// Belongs to Target
	// Searchable
	targetId: object
	
	// Searchable
	serviceId: object
}
``` 

## ActiveScene

A Scene activation in a Session
### Queries

- [GetActiveScenes](#getactivescenes)
- [GetActiveScenesByQuery](#getactivescenesbyquery)
- [GetActiveScenesByIds](#getactivescenesbyids)

```ts
{
	// Ensured for Session
	// Belongs to Session
	sessionId: object
	
	// Ensured for Scene
	// Belongs to Scene
	sceneId: object
	
	// Defaults to inactive
	buildStatus: object
	
	// the ID of the server that changed the state of the scene
	buildSource: object
	
	txId: object
	
	// ISO 8601 of last BuildOn for this scene
	lastBuiltOn: string
}
``` 

## Alert

An alert that can be displayed to the user.
### Queries

- [GetAlerts](#getalerts)
- [GetAlertsByQuery](#getalertsbyquery)
- [GetAlertsByIds](#getalertsbyids)
- [GetAlertsByEntities](#getalertsbyentities)

```ts
{
	// The ID of the entity that the alert is attached to
	entityId: object
	
	// The type of entity that the alert is attached to
	entityType: object
	
	// Belongs to Instance
	instanceId: object
	
	// Alert Level
	level: string
	
	// The message to display to the user
	message: string
	
	// The code of the alert's message
	code: string
}
``` 

## Appearance

An appearance of a scene on a calendar.
### Queries

- [GetAppearances](#getappearances)
- [GetAppearancesByIds](#getappearancesbyids)
- [GetAppearancesByQuery](#getappearancesbyquery)
- [GetAppearancesBySpaceId](#getappearancesbyspaceid)

```ts
{
	// Belongs to Scene
	sceneId: object
	
	// ISO DateTime string
	startTime: string
	
	// ISO DateTime string
	endTime: string
	
	// Belongs to Project
	scopeId: object
	
	// Belongs to Calendar
	calendarId: object
}
``` 

## Asset

An Asset Stub for future use
### Queries

- [GetAssets](#getassets)
- [GetAssetsByIds](#getassetsbyids)

```ts
{
	name: string
	
	directory: string
}
``` 

## AuditLog

An audit log entry recording a security or compliance event
### Queries

- [GetAuditLogs](#getauditlogs)
- [GetAuditLogsByOrganization](#getauditlogsbyorganization)
- [GetAuditLogsByUser](#getauditlogsbyuser)
- [GetAuditLogsByTarget](#getauditlogsbytarget)
- [SearchAuditLogs](#searchauditlogs)

```ts
{
	// Action that was performed
	action: object
	
	// Severity level: info, warning, error, critical
	severity: object
	
	// User who performed the action (null for system actions)
	actorUserId: object
	
	// Actor display name (cached for historical reference)
	actorName: string
	
	// Actor email (cached)
	actorEmail: string
	
	// Organization context where action occurred
	organizationId: object
	
	// Type of target entity
	targetType: string
	
	// ID of target entity
	targetId: object
	
	// Target entity name (cached)
	targetName: string
	
	// When the action occurred
	timestamp: string
	
	// IP address of actor
	ipAddress: string
	
	// User agent string
	userAgent: string
	
	// Human-readable description of what happened
	description: string
	
	// Additional structured data about the event
	metadata: object
	
	// Previous state (for update actions)
	previousState: object
	
	// New state (for update actions)
	newState: object
	
	// Whether action succeeded
	success: boolean
	
	// Error message if action failed
	errorMessage: string
}
``` 

## Binding

> WARNING: deprecated

direct binding of an emitter to a bundle
### Queries

- [GetBindings](#getbindings)
- [GetBindingsByIds](#getbindingsbyids)
- [GetBindingsByEmitterId](#getbindingsbyemitterid)
- [GetBindingsByScopeId](#getbindingsbyscopeid)

```ts
{
	// Belongs to Project
	scopeId: object
	
	// Belongs to Emitter
	emitterId: object
	
	// Belongs to Bundle
	bundleId: object
}
``` 

## BindingNode

A node in a scene graph that binds transforms data flow
### Queries

- [GetBindingNodes](#getbindingnodes)
- [GetBindingNodesByQuery](#getbindingnodesbyquery)
- [GetBindingNodesByIds](#getbindingnodesbyids)
- [GetBindingNodesInputConnectedToType](#getbindingnodesinputconnectedtotype)

```ts
{
	// Belongs to Scene
	sceneId: object
	
	// The type of binding node
	type: object
	
	// The name of the binding node
	name: string
	
	// Belongs to Target
	targetRefId: object
	
	// Belongs to Emitter
	emitterRefId: object
	
	// Belongs to Action
	actionRefId: object
	
	// Belongs to Scene
	buildOnRefId: object
	
	// Belongs to Scene
	buildOffRefId: object
	
	// Belongs to EventTrack
	fireEventTrackRefId: object
	
	// Belongs to Bundle
	bundleRefId: object
	
	// Belongs to GlobalVariable
	globalVariableRefId: object
}
``` 

## BindingNodeConnection

the connection between two binding nodes
### Queries

- [GetBindingNodeConnections](#getbindingnodeconnections)
- [GetBindingNodeConnectionsByQuery](#getbindingnodeconnectionsbyquery)
- [GetBindingNodeConnectionsByIds](#getbindingnodeconnectionsbyids)

```ts
{
	// Belongs to Scene
	sceneId: object
	
	startAnchorId: object
	
	endAnchorId: object
	
	// Belongs to BindingNode
	startNodeId: object
	
	// Belongs to BindingNode
	endNodeId: object
}
``` 

## BindingNodePosition

The position of a binding node the scene's node graph editor
### Queries

- [GetBindingNodePositions](#getbindingnodepositions)
- [GetBindingNodePositionsByQuery](#getbindingnodepositionsbyquery)
- [GetBindingNodePositionsByIds](#getbindingnodepositionsbyids)

```ts
{
	// Belongs to BindingNode
	nodeId: object
	
	x: number
	
	y: number
	
	w: number
	
	h: number
	
	structuredColumn: number
	
	structuredSlot: number
}
``` 

## BindingNodeValue

tracks the manual value of a binding node's anchor
### Queries

- [GetBindingNodeValues](#getbindingnodevalues)
- [GetBindingNodeValuesByQuery](#getbindingnodevaluesbyquery)

```ts
{
	// Belongs to BindingNode
	nodeId: object
	
	// node anchor for which the value is being set
	anchorId: object
	
	// the value manually entered into a binding node
	value: any
}
``` 

## Bundle

A bundle of payloads to be sent at the same time
### Queries

- [GetBundles](#getbundles)
- [GetBundlesByScopeId](#getbundlesbyscopeid)
- [GetBundlesByIds](#getbundlesbyids)

```ts
{
	// Belongs to Project
	scopeId: object
	
	// Owns many Payload
	payloadIds: array
}
``` 

## BundleStatus

> WARNING: deprecated

The status of a bundle in a session
### Queries

- [GetBundleStatuses](#getbundlestatuses)
- [GetBundleStatusesByQuery](#getbundlestatusesbyquery)
- [GetBundleStatusesByIds](#getbundlestatusesbyids)
- [GetBundleStatusesByBundlesAndConfig](#getbundlestatusesbybundlesandconfig)

```ts
{
	// Belongs to Session
	// Ensured for Session
	sessionId: object
	
	// Ensured for Bundle
	// Belongs to Bundle
	bundleId: object
	
	// Defaults to false
	armed: boolean
}
``` 

## Calendar

A calendar for a project
### Queries

- [GetCalendars](#getcalendars)
- [GetCalendarsByQuery](#getcalendarsbyquery)
- [GetCalendarsByIds](#getcalendarsbyids)

```ts
{
	// Ensured for Project
	// Belongs to Project
	scopeId: object
	
	// Defaults to Main Calendar
	name: string
}
``` 

## Calibration


### Queries

- [GetCalibrations](#getcalibrations)
- [GetCalibrationsByQuery](#getcalibrationsbyquery)
- [GetCalibrationsByIds](#getcalibrationsbyids)

```ts
{
	// Belongs to Camera
	cameraId: object
}
``` 

## CalibrationPoint


### Queries

- [GetCalibrationPoints](#getcalibrationpoints)
- [GetCalibrationPointsByQuery](#getcalibrationpointsbyquery)
- [GetCalibrationPointsByIds](#getcalibrationpointsbyids)

```ts
{
	// Belongs to Camera
	cameraId: object
	
	// Belongs to Point
	pointId: object
	
	// Belongs to Calibration
	calibrationId: object
}
``` 

## Camera


### Queries

- [GetCameras](#getcameras)
- [GetCamerasByQuery](#getcamerasbyquery)
- [GetCamerasByIds](#getcamerasbyids)

```ts
{
	// Belongs to Project
	projectId: object
}
``` 

## Client

A Myko Client connected to a Server
### Queries

- [GetClientsByIds](#getclientsbyids)
- [GetClientsByQuery](#getclientsbyquery)

```ts
{
	// Belongs to Server
	serverId: string
}
``` 

## ClusterTargetAssign


### Queries

- [GetClusterTargetAssigns](#getclustertargetassigns)
- [GetClusterTargetAssignsByQuery](#getclustertargetassignsbyquery)
- [GetClusterTargetAssignsByIds](#getclustertargetassignsbyids)

```ts
{
	// Belongs to Target
	targetId: object
	
	// Belongs to Instance
	instanceId: object
	
	clusterId: object
	
	// Belongs to Session
	sessionId: object
}
``` 

## ColorProfile


### Queries

- [GetColorProfiles](#getcolorprofiles)
- [GetColorProfilesByQuery](#getcolorprofilesbyquery)
- [GetColorProfilesByIds](#getcolorprofilesbyids)

```ts
{
	// Belongs to Project
	projectId: object
	
	// Belongs to Camera
	cameraId: object
}
``` 

## Composition


### Queries

- [GetCompositions](#getcompositions)
- [GetCompositionsByProjectId](#getcompositionsbyprojectid)
- [GetCompositionById](#getcompositionbyid)
- [GetCompositionsByIds](#getcompositionsbyids)

```ts
{
	// Composition name
	name: string
	
	// Composition description
	description: string
	
	// Belongs to Project
	projectId: object
	
	// Internal track for storing layers
	// Belongs to Track
	trackId: object
	
	// Output width in pixels
	width: number
	
	// Output height in pixels
	height: number
	
	// Background color (hex)
	backgroundColor: string
	
	// Composition enabled/disabled
	enabled: boolean
	
	// Order for stacking compositions
	order: number
	
	// Is this the main/default composition
	isMain: boolean
}
``` 

## Constraint

In progress, subject to change. The intention with the start and end time is to allow for times of day throughout a series of days, but probably needs to be reworked
### Queries

- [GetConstraints](#getconstraints)
- [GetConstraintsByIds](#getconstraintsbyids)
- [GetConstraintsByQuery](#getconstraintsbyquery)

```ts
{
	// Belongs to Project
	scopeId: object
	
	name: string
	
	// ISO Time string
	activeStartTime: string
	
	// ISO Time string
	activeEndTime: string
	
	// ISO Date string
	activeStartDate: string
	
	// ISO Date string
	activeEndDate: string
	
	data: object
	
	// Belongs to Calendar
	calendarId: object
	
	// Client/advertiser name for contract tracking
	clientName: string
	
	// Contract reference number
	contractRef: string
	
	// Priority weight for soft constraint resolution (default 1.0, higher = more important)
	weight: number
	
	// Whether this is a hard constraint that must be satisfied (default false)
	isHard: boolean
}
``` 

## Cue


### Queries

- [GetCues](#getcues)
- [GetCuesByQuery](#getcuesbyquery)
- [GetCuesByIds](#getcuesbyids)

```ts
{
	// Cue type and data
	cueType: object
	
	// Belongs to Sequence
	sequenceId: object
	
	// Previous cue ID
	previousCueId: object
}
``` 

## CuePlayback


### Queries

- [GetCuePlaybacks](#getcueplaybacks)
- [GetCuePlaybacksByQuery](#getcueplaybacksbyquery)
- [GetCuePlaybacksByIds](#getcueplaybacksbyids)
- [GetCuePlaybacksByCueIds](#getcueplaybacksbycueids)

```ts
{
	// Belongs to Cue
	cueId: object
	
	// Belongs to Sequence
	sequenceId: object
	
	// Belongs to Session
	sessionId: object
	
	state: object
}
``` 

## Curve


### Queries

- [GetCurves](#getcurves)
- [GetCurvesByQuery](#getcurvesbyquery)
- [GetCurvesByIds](#getcurvesbyids)

```ts
{
	// curve slices
	slices: array
	
	// Belongs to Project
	scopeId: object
	
	// name
	name: string
}
``` 

## DataTransfer

A data transfer between a binding and a payload
### Queries

- [GetDataTransfers](#getdatatransfers)
- [GetDataTransfersByQuery](#getdatatransfersbyquery)
- [GetDataTransfersByIds](#getdatatransfersbyids)

```ts
{
	// Belongs to Binding
	bindingId: object
	
	// Belongs to Payload
	payloadId: object
	
	// schema for the path to the payload's data
	path: string
	
	// replacement template string
	replacement: string
}
``` 

## DeviceOwnership

Ownership record for a device, supporting claiming and sharing
### Queries

- [GetDeviceOwnership](#getdeviceownership)
- [GetDeviceOwnershipByMachine](#getdeviceownershipbymachine)
- [GetDeviceOwnershipsByOrganization](#getdeviceownershipsbyorganization)
- [GetDeviceOwnershipsByUser](#getdeviceownershipsbyuser)
- [GetUnclaimedDevices](#getunclaimeddevices)
- [GetPendingClaims](#getpendingclaims)

```ts
{
	// Machine ID this ownership record is for
	machineId: object
	
	// Organization that owns this device
	organizationId: object
	
	// Primary owner user ID
	ownerUserId: object
	
	// Ownership status: unclaimed, pending, claimed, shared, transferred
	status: object
	
	// Claim code for device registration (one-time use)
	claimCode: string
	
	// When the claim code expires
	claimCodeExpiresAt: string
	
	// User who submitted the claim (for pending status)
	claimantUserId: object
	
	// When the claim was submitted
	claimSubmittedAt: string
	
	// User ID ownership is being transferred to
	transferToUserId: object
	
	// When transfer was initiated
	transferInitiatedAt: string
	
	// When ownership was established
	claimedAt: string
	
	// Notes about this device
	notes: string
}
``` 

## DeviceShare

A share granting a user access to a device
### Queries

- [GetDeviceShares](#getdeviceshares)
- [GetDeviceSharesByMachine](#getdevicesharesbymachine)
- [GetDeviceSharesByUser](#getdevicesharesbyuser)

```ts
{
	// Machine ID being shared
	machineId: object
	
	// User ID receiving shared access
	sharedWithUserId: object
	
	// Permission level: view, control, manage
	shareLevel: object
	
	// User ID who created the share
	sharedByUserId: object
	
	// When the share was created
	sharedAt: string
	
	// Optional expiration for temporary shares
	expiresAt: string
	
	// Note about why device was shared
	note: string
}
``` 

## Directory


### Queries

- [GetDirectories](#getdirectories)
- [GetDirectoriesByIds](#getdirectoriesbyids)

```ts
{
	
}
``` 

## DMXUniverse


### Queries

- [GetDMXUniverses](#getdmxuniverses)
- [GetDMXUniversesByQuery](#getdmxuniversesbyquery)
- [GetDMXUniversesByIds](#getdmxuniversesbyids)
- [GetDMXUniversesByProjectId](#getdmxuniversesbyprojectid)
- [GetDMXUniverseByNumber](#getdmxuniversebynumber)

```ts
{
	
}
``` 

## Emitter

A changing value, or event, published from a target
### Queries

- [GetEmitters](#getemitters)
- [GetEmittersByIds](#getemittersbyids)
- [GetEmittersByQuery](#getemittersbyquery)

```ts
{
	// Searchable
	name: string
	
	// Searchable
	// Belongs to Target
	targetId: object
	
	// Searchable
	serviceId: object
	
	schema: JSONSchema
}
``` 

## EventTrack

An Event Track for a Scene
Event Tracks aligned at end will delay so their final keyframe coincides with the last keyframe of the longest Event Track in their group
### Queries

- [GetEventTracks](#geteventtracks)
- [GetEventTracksByQuery](#geteventtracksbyquery)
- [GetEventTracksByIds](#geteventtracksbyids)
- [GetStartEventTracksByScene](#getstarteventtracksbyscene)
- [GetEndEventTracksByScene](#getendeventtracksbyscene)

```ts
{
	name: string
	
	timeMode: object
	
	sourceMode: object
	
	locked: boolean
}
``` 

## EventTrackLayer


### Queries

- [GetEventTrackLayers](#geteventtracklayers)
- [GetEventTrackLayersByQuery](#geteventtracklayersbyquery)
- [GetEventTrackLayersByIds](#geteventtracklayersbyids)

```ts
{
	// Belongs to EventTrack
	eventTrackId: object
	
	name: string
	
	type: object
}
``` 

## EventTrackPlayback

An Event Track Playback in a Session
### Queries

- [GetEventTrackPlaybacks](#geteventtrackplaybacks)
- [GetEventTrackPlaybacksByQuery](#geteventtrackplaybacksbyquery)
- [GetEventTrackPlaybacksByIds](#geteventtrackplaybacksbyids)

```ts
{
	// Belongs to EventTrack
	eventTrackId: object
	
	// Belongs to Session
	sessionId: object
	
	// Belongs to BindingNode
	bindingNodeId: object
	
	// ISO DateTime string
	startTime: string
	
	// transactionId of start command
	startTxIds: array
}
``` 

## ExecLog


### Queries

- [GetExecLogs](#getexeclogs)
- [GetExecLogsByQuery](#getexeclogsbyquery)
- [GetExecLogsByIds](#getexeclogsbyids)

```ts
{
	// Belongs to Machine
	machineId: object
}
``` 

## File


### Queries

- [GetFiles](#getfiles)
- [GetFilesByIds](#getfilesbyids)
- [GetFilesByMachineId](#getfilesbymachineid)
- [AvailableFilesForQuery](#availablefilesforquery)

```ts
{
	// Searchable
	assetPath: string
	
	// Searchable
	localPath: string
	
	// Belongs to Machine
	machineId: object
}
``` 

## FileTransfer


### Queries

- [GetFileTransfers](#getfiletransfers)
- [GetFileTransfersByQuery](#getfiletransfersbyquery)
- [GetFileTransfersByIds](#getfiletransfersbyids)

```ts
{
	// asset path being transferred (no version)
	assetPath: string
	
	// version of the asset being transferred
	version: number
	
	// Belongs to Machine
	destinationMachineId: object
	
	// progress of the file transfer
	progress: number
}
``` 

## Fixture


### Queries

- [GetFixtures](#getfixtures)
- [GetFixturesByQuery](#getfixturesbyquery)
- [GetFixturesByIds](#getfixturesbyids)
- [GetFixturesByProjectId](#getfixturesbyprojectid)
- [GetFixturesByFixtureTypeId](#getfixturesbyfixturetypeid)

```ts
{
	
}
``` 

## FixtureCalibration


### Queries

- [GetFixtureCalibrations](#getfixturecalibrations)
- [GetFixtureCalibrationsByQuery](#getfixturecalibrationsbyquery)
- [GetFixtureCalibrationsByIds](#getfixturecalibrationsbyids)

```ts
{
	// Belongs to Project
	projectId: object
	
	// Belongs to FixtureType
	fixtureTypeId: object
}
``` 

## FixtureGroup


### Queries

- [GetFixtureGroups](#getfixturegroups)
- [GetFixtureGroupsByQuery](#getfixturegroupsbyquery)
- [GetFixtureGroupsByIds](#getfixturegroupsbyids)
- [GetFixtureGroupsByProjectId](#getfixturegroupsbyprojectid)
- [GetFixtureGroupsByFixtureId](#getfixturegroupsbyfixtureid)

```ts
{
	
}
``` 

## FixturePreset


### Queries

- [GetFixturePresets](#getfixturepresets)
- [GetFixturePresetsByQuery](#getfixturepresetsbyquery)
- [GetFixturePresetsByIds](#getfixturepresetsbyids)
- [GetFixturePresetsByProjectId](#getfixturepresetsbyprojectid)

```ts
{
	
}
``` 

## FixtureType


### Queries

- [GetFixtureTypes](#getfixturetypes)
- [GetFixtureTypesByQuery](#getfixturetypesbyquery)
- [GetFixtureTypesByIds](#getfixturetypesbyids)

```ts
{
	
}
``` 

## GlobalVariable


### Queries

- [GetGlobalVariables](#getglobalvariables)
- [GetGlobalVariablesByQuery](#getglobalvariablesbyquery)
- [GetGlobalVariablesByIds](#getglobalvariablesbyids)
- [GetGlobalVariablesSetByScene](#getglobalvariablessetbyscene)
- [GetGlobalVariablesConsumedByScene](#getglobalvariablesconsumedbyscene)
- [GetGlobalVariablesInScene](#getglobalvariablesinscene)

```ts
{
	// Searchable
	name: string
	
	resolutionStrategy: object
	
	// Belongs to Project
	scopeId: object
}
``` 

## Instance

An Instance of a Service
### Queries

- [GetInstances](#getinstances)
- [GetInstancesByQuery](#getinstancesbyquery)
- [GetInstancesByIds](#getinstancesbyids)

```ts
{
	name: string
	
	serviceId: object
	
	// Auto Client ID
	clientId: object
	
	serviceTypeCode: string
	
	// #rrggbb
	color: object
	
	machineId: object
	
	status: Instance Status
	
	message: string
	
	clusterId: object
}
``` 

## InstanceAssign

An Instance assignment in a Session
### Queries

- [GetInstanceAssigns](#getinstanceassigns)
- [GetInstanceAssignsByIds](#getinstanceassignsbyids)
- [GetInstanceAssignsByQuery](#getinstanceassignsbyquery)

```ts
{
	// Belongs to Session
	sessionId: object
	
	// Belongs to Instance
	instanceId: object
	
	serviceId: object
}
``` 

## InstanceClusterAssign

An Instance assignment by cluster in a Session
### Queries

- [GetInstanceClusterAssigns](#getinstanceclusterassigns)
- [GetInstanceClusterAssignsByQuery](#getinstanceclusterassignsbyquery)
- [GetInstanceClusterAssignsByIds](#getinstanceclusterassignsbyids)

```ts
{
	clusterId: object
	
	// Belongs to Session
	sessionId: object
	
	serviceId: object
}
``` 

## Invite

An invitation for a user to join an organization or resource
### Queries

- [GetInvites](#getinvites)
- [GetInviteByCode](#getinvitebycode)
- [GetInvitesByEntity](#getinvitesbyentity)
- [GetInvitesByEmail](#getinvitesbyemail)
- [GetPendingInvitesByOrganization](#getpendinginvitesbyorganization)

```ts
{
	// Type of invitation: organization, project, network
	inviteType: object
	
	// ID of the entity being invited to
	entityId: object
	
	// Email address of invited user
	email: string
	
	// Role to grant upon acceptance
	roleType: object
	
	// Invite status: pending, accepted, declined, expired, revoked
	status: object
	
	// User ID who created the invite
	invitedByUserId: object
	
	// When the invite was created
	createdAt: string
	
	// When the invite expires
	expiresAt: string
	
	// Unique invite code/token
	inviteCode: string
	
	// Personal message included with invite
	message: string
	
	// When the invite was accepted/declined
	resolvedAt: string
	
	// User ID who accepted the invite (created or existing)
	acceptedByUserId: object
}
``` 

## Keyframe

A keyframe on an Event Track
### Queries

- [GetKeyframes](#getkeyframes)
- [GetKeyframesByQuery](#getkeyframesbyquery)
- [GetKeyframesByIds](#getkeyframesbyids)

```ts
{
	// Belongs to EventTrack
	eventTrackId: object
	
	// Belongs to EventTrackLayer
	layerId: object
	
	// milliseconds from the Event Track's start
	time: number
	
	data: any
}
``` 

## Layer


### Queries

- [GetLayers](#getlayers)
- [GetLayersByTrackId](#getlayersbytrackid)
- [GetLayerById](#getlayerbyid)

```ts
{
	// Layer type
	type: object
	
	// Layer name
	name: string
	
	// Layer description
	description: string
	
	// Belongs to Track
	trackId: object
	
	// Belongs to Mapping
	mappingId: object
	
	// Layer order in track (higher = on top)
	order: number
	
	// Layer opacity (0-1)
	opacity: number
	
	// Blend mode for compositing
	blendMode: object
	
	// Layer enabled/disabled
	enabled: boolean
	
	// Layer solo (only render this layer)
	solo: boolean
	
	// Layer locked (prevent editing)
	locked: boolean
	
	// Start time in milliseconds
	startTime: number
	
	// Duration in milliseconds (undefined = infinite)
	duration: number
	
	// Layer color for UI
	color: string
}
``` 

## LayerGroup


### Queries

- [GetLayerGroups](#getlayergroups)
- [GetLayerGroupsByCompositionId](#getlayergroupsbycompositionid)
- [GetLayerGroupById](#getlayergroupbyid)
- [GetLayerGroupsByIds](#getlayergroupsbyids)

```ts
{
	// Layer group name
	name: string
	
	// Layer group description
	description: string
	
	// Belongs to Composition
	compositionId: object
	
	// Ordered list of child layer IDs
	layerIds: array
	
	// Order in the composition stack (higher = on top)
	order: number
	
	// Group opacity (0-1)
	opacity: number
	
	// Blend mode for the group output
	blendMode: object
	
	// Group enabled/disabled
	enabled: boolean
	
	// UI collapsed state
	collapsed: boolean
	
	// Group color for UI
	color: string
}
``` 

## LEDWall


### Queries

- [GetLEDWalls](#getledwalls)
- [GetLEDWallsByProjectId](#getledwallsbyprojectid)
- [GetLEDWallsByIds](#getledwallsbyids)

```ts
{
	// LED wall name
	name: string
	
	// LED wall description
	description: string
	
	// Belongs to Project
	projectId: object
	
	// X position in meters
	x: number
	
	// Y position in meters
	y: number
	
	// Z position in meters
	z: number
	
	// X rotation in degrees
	rotX: number
	
	// Y rotation in degrees
	rotY: number
	
	// Z rotation in degrees
	rotZ: number
	
	// Width in meters
	width: number
	
	// Height in meters
	height: number
	
	// Resolution width in pixels
	resolutionWidth: number
	
	// Resolution height in pixels
	resolutionHeight: number
	
	// Pixel pitch in millimeters
	pixelPitch: number
	
	// DMX universe
	universe: number
	
	// DMX start address
	address: number
	
	// LED mode (RGB or RGBW)
	mode: object
	
	// Content stream ID (for video texture)
	streamId: object
	
	// Video engine screen ID (for mapped video content)
	screenId: object
	
	// VFX Graph ID (for shader-based content)
	vfxGraphId: object
	
	// Rive Project ID (for Rive animation content)
	riveProjectId: object
	
	// Rive artboard name to display
	riveArtboardName: string
	
	// Rive state machine name (for interactive animations)
	riveStateMachineName: string
	
	// Rive animation name (for direct animation playback)
	riveAnimationName: string
	
	// Rive playback mode: state-machine, animation, or static
	rivePlaybackMode: object
	
	// How Rive content fits the LED wall
	riveFit: object
	
	// Rive content alignment within bounds
	riveAlignment: object
	
	// Color for preview/testing
	color: string
	
	// Brightness (0-1)
	brightness: number
	
	// Enable/disable rendering
	enabled: boolean
	
	// URL to imported 3D geometry in Asset Store
	geometryUrl: string
	
	// Render mode: imported, procedural, or hybrid
	renderMode: object
}
``` 

## LensFile


### Queries

- [GetLensFiles](#getlensfiles)
- [GetLensFilesByQuery](#getlensfilesbyquery)
- [GetLensFilesByIds](#getlensfilesbyids)

```ts
{
	
}
``` 

## LinkExec


### Queries

- [GetLinkExecs](#getlinkexecs)
- [GetLinkExecsByQuery](#getlinkexecsbyquery)
- [GetLinkExecsByIds](#getlinkexecsbyids)

```ts
{
	// Belongs to Machine
	machineId: object
	
	// The name of the executor
	name: string
	
	// The path of the executor
	path: string
}
``` 

## LinkExecRunner


### Queries

- [GetLinkExecRunners](#getlinkexecrunners)
- [GetLinkExecRunnersByQuery](#getlinkexecrunnersbyquery)
- [GetLinkExecRunnersByIds](#getlinkexecrunnersbyids)

```ts
{
	// Belongs to Machine
	machineId: object
}
``` 

## LinkLog


### Queries

- [GetLinkLogs](#getlinklogs)
- [GetLinkLogsByQuery](#getlinklogsbyquery)
- [GetLinkLogsByIds](#getlinklogsbyids)

```ts
{
	// Belongs to Machine
	machineId: object
	
	// ISO DateTime string
	timestamp: string
}
``` 

## Location


### Queries

- [GetLocations](#getlocations)
- [GetLocationsByQuery](#getlocationsbyquery)
- [GetLocationsByIds](#getlocationsbyids)

```ts
{
	
}
``` 

## Log


### Queries

- [GetLogs](#getlogs)

```ts
{
	text: string
	
	level: object
	
	data: object
	
	timestamp: string
	
	serverId: object
	
	loggerName: string
}
``` 

## Machine

A Machine that hosts an instance or acts as a network bridge device
### Queries

- [GetMachines](#getmachines)
- [GetMachinesByQuery](#getmachinesbyquery)
- [GetMachinesByIds](#getmachinesbyids)
- [GetMachineById](#getmachinebyid)
- [GetMachineByJnOtpAccountName](#getmachinebyjnotpaccountname)
- [GetMachineBySerialNumber](#getmachinebyserialnumber)

```ts
{
	name: string
	
	dnsName: string
	
	execName: string
	
	// xxx.xxx.xxx.xxx
	addresses: array
	
	// Auto Client ID
	clientId: object
	
	// Product type: rshipLink, theBRIDGE, desktop, server
	productType: object
	
	// Hardware model ID for bridge devices
	modelId: string
	
	// Device serial number
	serialNumber: string
	
	// WAN MAC address
	wanMacAddress: string
	
	// Serial communication type for hardware devices
	serialCommunicationType: object
	
	// Daemon/agent version
	daemonVersion: string
	
	// VPN provider status array
	vpnProviders: array
	
	// Machine capabilities
	capabilities: object
	
	// Original JN device ID for migration reference
	jnDeviceId: string
	
	// JN OTP account name for legacy auth
	jnOtpAccountName: string
	
	// JN OTP secret (encrypted) for legacy auth
	jnOtpSecret: string
	
	// Whether migration to native rship auth is complete
	migrationComplete: boolean
}
``` 

## MachineStatus

Connection Status of a machine
### Queries

- [GetMachineStatuses](#getmachinestatuses)
- [GetMachineStatusesByQuery](#getmachinestatusesbyquery)
- [GetMachineStatusesByIds](#getmachinestatusesbyids)
- [GetMachineStatusesByMachineId](#getmachinestatusesbymachineid)

```ts
{
	// Belongs to Machine
	machineId: object
	
	// The address of the server that is in contact with the machine
	serverAddress: string
	
	// most recent ping time
	ping: object
	
	alive: boolean
	
	// ISO DateTime string
	lastSeen: string
}
``` 

## Mapping


### Queries

- [GetMappings](#getmappings)
- [GetMappingsByProjectId](#getmappingsbyprojectid)
- [GetMappingById](#getmappingbyid)
- [GetMappingsByType](#getmappingsbytype)
- [GetMappingsByScreenId](#getmappingsbyscreenid)

```ts
{
	// Mapping name
	name: string
	
	// Mapping description
	description: string
	
	// Belongs to Project
	projectId: object
	
	// Mapping type
	type: object
	
	// Mapping configuration (type-specific)
	config: object
	
	// Screen IDs this mapping targets
	screenIds: array
	
	// Mapping enabled/disabled
	enabled: boolean
	
	// Mapping order (for multiple mappings on same screens)
	order: number
	
	// Linked physical display ID (from Tauri display enumeration)
	physicalDisplayId: string
	
	// NVIDIA display ID for NVIDIA-specific operations
	nvidiaDisplayId: string
	
	// Physical display name (cached for display when disconnected)
	physicalDisplayName: string
	
	// Physical display connection type (cached)
	physicalDisplayConnection: string
	
	// Link/sync status
	linkStatus: object
	
	// Last sync error message
	lastSyncError: string
	
	// Last sync timestamp
	lastSyncAt: number
}
``` 

## Measurement


### Queries

- [GetMeasurements](#getmeasurements)
- [GetMeasurementsByProjectId](#getmeasurementsbyprojectid)
- [GetMeasurementsByIds](#getmeasurementsbyids)

```ts
{
	// Measurement name
	name: string
	
	// Measurement description
	description: string
	
	// Measurement type (distance, angle, area)
	type: object
	
	// Array of 3D points defining the measurement
	points: array
	
	// Calculated value (distance in meters, angle in degrees, area in square meters)
	value: number
	
	// Optional unit override (m, cm, mm, degrees, sq m, etc)
	unit: string
	
	// Color for visualization
	color: string
	
	// Belongs to Project
	projectId: object
}
``` 

## Network

A VPN network that machines can join for secure connectivity
### Queries

- [GetNetworks](#getnetworks)
- [GetNetworkById](#getnetworkbyid)
- [GetNetworksByOrganization](#getnetworksbyorganization)
- [GetNetworkByVpnId](#getnetworkbyvpnid)

```ts
{
	// Human-readable name for the network
	name: string
	
	// Optional description
	description: string
	
	// Organization that owns this network
	organizationId: object
	
	// VPN provider: zerotier, tailscale, or netbird
	vpnProvider: object
	
	// VPN network identifier (e.g., ZeroTier network ID)
	vpnNetworkId: string
	
	// Control plane URL for self-hosted providers
	vpnControlUrl: string
	
	// Bridge network CIDR (e.g., "192.168.0.0/24")
	bridgeNetwork: string
	
	// Devices auto-join this network on registration
	autoJoin: boolean
	
	// New devices require manual approval
	requireApproval: boolean
	
	// Original JN network ID for migration reference
	jnNetworkId: string
}
``` 

## NetworkMember

A machine membership in a VPN network
### Queries

- [GetNetworkMembers](#getnetworkmembers)
- [GetNetworkMembersByNetwork](#getnetworkmembersbynetwork)
- [GetNetworkMembersByMachine](#getnetworkmembersbymachine)
- [GetNetworkMember](#getnetworkmember)

```ts
{
	// Network this membership belongs to
	networkId: object
	
	// Machine that is a member of the network
	machineId: object
	
	// Current membership status
	status: object
	
	// VPN-assigned IP address(es)
	vpnAddresses: array
	
	// Hostname on the VPN network
	vpnHostname: string
	
	// When the machine joined the network
	joinedAt: string
	
	// Last time machine was seen on the network
	lastSeenAt: string
	
	// Error message if status is error
	errorMessage: string
}
``` 

## Organization

An organization that owns users, devices, and networks
### Queries

- [GetOrganizations](#getorganizations)
- [GetOrganizationById](#getorganizationbyid)
- [GetOrganizationBySlug](#getorganizationbyslug)
- [GetOrganizationsByUser](#getorganizationsbyuser)

```ts
{
	// Organization display name
	name: string
	
	// URL-friendly slug (unique)
	slug: string
	
	// Optional description
	description: string
	
	// Organization logo URL
	logoUrl: string
	
	// Primary contact email
	contactEmail: string
	
	// Subscription tier: free, starter, professional, enterprise
	subscriptionTier: object
	
	// Stripe customer ID for billing
	stripeCustomerId: string
	
	// Subscription status: active, past_due, canceled, trialing
	subscriptionStatus: string
	
	// Maximum number of users allowed
	maxUsers: number
	
	// Maximum number of devices allowed
	maxDevices: number
	
	// Maximum number of networks allowed
	maxNetworks: number
	
	// Require approval for new members
	requireMemberApproval: boolean
	
	// Allow public device claiming
	allowPublicClaiming: boolean
	
	// Default role for new members
	defaultMemberRole: string
	
	// Organization creation timestamp
	createdAt: string
	
	// ID of user who created the organization
	createdBy: object
}
``` 

## OverviewNode


### Queries

- [GetOverviewNodes](#getoverviewnodes)
- [GetOverviewNodesByQuery](#getoverviewnodesbyquery)
- [GetOverviewNodesByIds](#getoverviewnodesbyids)

```ts
{
	// Belongs to Scene
	sceneId: object
	
	// Belongs to Scene
	refSceneId: object
	
	// Belongs to Project
	scopeId: object
	
	// position
	position: object
}
``` 

## Pane

An individual pane within a window group layout
### Queries

- [GetPanes](#getpanes)
- [GetPanesByIds](#getpanesbyids)
- [GetPanesByWindowGroupId](#getpanesbywindowgroupid)
- [GetPanesByOwnerId](#getpanesbyownerid)

```ts
{
	// Ensured for User
	ownerId: object
	
	windowGroupId: object
	
	projectId: object
	
	sessionId: object
	
	// Defaults to visualizer
	activeView: object
	
	// Defaults to [object Object]
	viewState: object
	
	// Defaults to 
	selectedTargets: array
	
	// Defaults to 
	selectedBundles: array
	
	// Defaults to false
	isPoppedOut: boolean
}
``` 

## Payload

A payload to be sent to an instance that will invoke an action
### Queries

- [GetPayloads](#getpayloads)
- [GetPayloadsByIds](#getpayloadsbyids)
- [GetPayloadsByServiceIds](#getpayloadsbyserviceids)
- [GetArmedPayloadsForInstances](#getarmedpayloadsforinstances)

```ts
{
	// Belongs to Project
	scopeId: object
	
	// Belongs to Target
	targetId: object
	
	// Belongs to Action
	actionId: object
	
	prepareKey: string
	
	// should fit the schema of the actioin referenced by actionId
	data: any
}
``` 

## PayloadStatus

A Payload Status in a Session
### Queries

- [GetPayloadStatuss](#getpayloadstatuss)
- [GetPayloadStatussByQuery](#getpayloadstatussbyquery)
- [GetPayloadStatussByIds](#getpayloadstatussbyids)

```ts
{
	// Ensured for Session
	// Belongs to Session
	sessionId: object
	
	// Ensured for Payload
	// Belongs to Payload
	payloadId: object
	
	// Defaults to false
	armed: boolean
}
``` 

## Playlist

A Playlist of Scenes that can be instantiated on a Calendar
### Queries

- [GetPlaylists](#getplaylists)
- [GetPlaylistsByQuery](#getplaylistsbyquery)
- [GetPlaylistsByIds](#getplaylistsbyids)

```ts
{
	// Searchable
	// The name of the Playlist
	name: string
	
	// Belongs to Project
	scopeId: object
	
	// ISO Duration string
	// autocalculated
	duration: string
	
	// tag ids
	tags: string[]
	
	description: string
}
``` 

## PlaylistAppearance

A Playlist appearance on a Calendar
### Queries

- [GetPlaylistAppearances](#getplaylistappearances)
- [GetPlaylistAppearancesByQuery](#getplaylistappearancesbyquery)
- [GetPlaylistAppearancesByIds](#getplaylistappearancesbyids)

```ts
{
	// Belongs to Playlist
	playlistId: object
	
	// Belongs to Calendar
	calendarId: object
	
	// ISO DateTime string
	startTime: string
}
``` 

## PlaylistItem

A Scene's timing in a Playlist
### Queries

- [GetPlaylistItems](#getplaylistitems)
- [GetPlaylistItemsByQuery](#getplaylistitemsbyquery)
- [GetPlaylistItemsByIds](#getplaylistitemsbyids)

```ts
{
	// Belongs to Playlist
	playlistId: object
	
	// Belongs to Scene
	sceneId: object
	
	// ISO Duration string, offset from start of playlist
	startOffset: string
	
	// ISO Duration string, offset from start of playlist
	endOffset: string
}
``` 

## Point


### Queries

- [GetPoints](#getpoints)
- [GetPointsByQuery](#getpointsbyquery)
- [GetPointsByIds](#getpointsbyids)

```ts
{
	// X coordinate
	x: number
	
	// Y coordinate
	y: number
	
	// Z coordinate
	z: number
	
	// Point name
	name: string
	
	// Point description
	description: string
	
	// Belongs to Project
	projectId: object
}
``` 

## Port

A network interface/port on a machine
### Queries

- [GetPorts](#getports)
- [GetPortsByMachine](#getportsbymachine)
- [GetPortById](#getportbyid)
- [GetPortsByNetwork](#getportsbynetwork)

```ts
{
	// Machine this port belongs to
	machineId: object
	
	// Port index (0-based)
	index: number
	
	// Human-readable port name
	name: string
	
	// OS interface name (e.g., eth0, en0)
	interfaceName: string
	
	// Type of interface
	interfaceType: object
	
	// MAC address of the interface
	macAddress: string
	
	// Current IP address
	ipAddress: string
	
	// Link speed in Mbps
	linkSpeed: number
	
	// Network this port is bridged to
	bridgedNetworkId: object
	
	// Status of the bridge configuration
	bridgeStatus: object
	
	// Error message if bridgeStatus is error
	bridgeError: string
	
	// Original JN port ID for migration reference
	jnPortId: string
}
``` 

## Project

A Project - the scope of most data in Rocketship
### Queries

- [GetProjects](#getprojects)
- [GetProjectsByIds](#getprojectsbyids)

```ts
{
	name: string
	
	description: string
}
``` 

## Prop


### Queries

- [GetProps](#getprops)
- [GetPropsByQuery](#getpropsbyquery)
- [GetPropsByProjectId](#getpropsbyprojectid)
- [GetPropsByIds](#getpropsbyids)

```ts
{
	// Prop name
	name: string
	
	// Prop description
	description: string
	
	// Belongs to Project
	projectId: object
	
	// X coordinate in meters
	x: number
	
	// Y coordinate in meters
	y: number
	
	// Z coordinate in meters
	z: number
	
	// Rotation around X axis in degrees
	rotX: number
	
	// Rotation around Y axis in degrees
	rotY: number
	
	// Rotation around Z axis in degrees
	rotZ: number
	
	// Scale factor for X axis
	scaleX: number
	
	// Scale factor for Y axis
	scaleY: number
	
	// Scale factor for Z axis
	scaleZ: number
	
	// URL to imported 3D geometry in Asset Store
	geometryUrl: string
	
	// Original source format of the geometry
	sourceFormat: string
	
	// Render mode: imported, procedural, or hybrid
	renderMode: object
	
	// Color tint for the prop (hex)
	color: string
	
	// Whether the prop is visible in the visualizer
	visible: boolean
}
``` 

## Pulse

A Pulse of data sent by an Emitter
### Queries

- [GetPulsesByIds](#getpulsesbyids)
- [GetPulsesByEmitterId](#getpulsesbyemitterid)

```ts
{
	// Belongs to Emitter
	emitterId: object
	
	data: any
	
	// unix timestamp
	timestamp: number
	
	// Auto Client ID
	clientId: object
}
``` 

## RenderCluster

A cluster of browser-based render nodes for distributed VFX rendering
### Queries

- [GetRenderClusters](#getrenderclusters)
- [GetRenderClustersByProjectId](#getrenderclustersbyprojectid)
- [GetRenderClusterById](#getrenderclusterbyid)
- [GetRenderClustersByStatus](#getrenderclustersbystatus)

```ts
{
	// Project this cluster belongs to
	// Belongs to Project
	projectId: object
	
	// Cluster name
	name: string
	
	// Cluster description
	description: string
	
	// Current coordinator node ID
	coordinatorNodeId: object
	
	// Target frames per second
	targetFps: number
	
	// Current frame number
	currentFrame: number
	
	// Cluster status
	status: object
	
	// Workload distribution mode
	distributionMode: object
	
	// Maximum nodes allowed in cluster
	maxNodes: number
	
	// Cluster color for UI
	color: string
	
	// Last frame sync timestamp
	lastSyncTimestamp: number
	
	// Average frame latency in milliseconds
	avgLatency: number
	
	// Cluster is paused
	paused: boolean
}
``` 

## RenderNode

A browser-based render node in a cluster
### Queries

- [GetRenderNodes](#getrendernodes)
- [GetRenderNodesByClusterId](#getrendernodesbyclusterid)
- [GetRenderNodeById](#getrendernodebyid)
- [GetRenderNodesByStatus](#getrendernodesbystatus)
- [GetRenderNodeByBrowserFingerprint](#getrendernodebybrowserfingerprint)

```ts
{
	// Cluster this node belongs to
	// Belongs to RenderCluster
	clusterId: object
	
	// Machine/hostname identifier
	machineId: string
	
	// Browser fingerprint for uniqueness
	browserFingerprint: string
	
	// Human-readable node name
	name: string
	
	// GPU information (JSON serialized)
	gpuInfo: string
	
	// Node status
	status: object
	
	// Last heartbeat timestamp
	lastHeartbeat: number
	
	// Average frame render time in milliseconds
	avgFrameTime: number
	
	// Current FPS
	currentFps: number
	
	// GPU memory usage in MB
	gpuMemoryUsage: number
	
	// Error message if status is "error"
	errorMessage: string
	
	// Assigned feed rectangles (JSON serialized)
	assignedRegions: string
	
	// Assigned screen IDs
	assignedScreenIds: string
	
	// MediaMTX stream endpoint
	streamEndpoint: string
	
	// WebRTC peer ID for data channel
	webrtcPeerId: string
	
	// Clock offset from coordinator in milliseconds
	clockOffset: number
	
	// Join timestamp
	joinedAt: number
	
	// Is this node the coordinator candidate
	isCoordinatorCandidate: boolean
}
``` 

## ResolutionStrategy


### Queries

- [GetResolutionStrategys](#getresolutionstrategys)
- [GetResolutionStrategysByQuery](#getresolutionstrategysbyquery)
- [GetResolutionStrategysByIds](#getresolutionstrategysbyids)

```ts
{
	name: string
	
	strategy: object
}
``` 

## RiveLayer

A Rive animation layer in the timeline
### Queries

- [GetRiveLayers](#getrivelayers)
- [GetRiveLayersByTrackId](#getrivelayersbytrackid)
- [GetRiveLayerById](#getrivelayerbyid)
- [GetRiveLayersByProjectId](#getrivelayersbyprojectid)

```ts
{
	// Track this layer belongs to
	// Belongs to Track
	trackId: object
	
	// Rive project asset
	// Belongs to RiveProject
	riveProjectId: object
	
	// Output mapping
	// Belongs to Mapping
	mappingId: object
	
	// Layer name
	name: string
	
	// Layer type (always "rive")
	type: string
	
	// Layer order in track (higher = on top)
	order: number
	
	// Start time in milliseconds
	startTime: number
	
	// Duration in milliseconds (undefined = infinite)
	duration: number
	
	// Layer enabled/disabled
	enabled: boolean
	
	// Layer solo (only render this layer)
	solo: boolean
	
	// Layer locked (prevent editing)
	locked: boolean
	
	// Layer opacity (0-1)
	opacity: number
	
	// Blend mode for compositing
	blendMode: object
	
	// Layer color for UI
	color: string
	
	// Selected artboard name
	artboardName: string
	
	// Selected state machine name (if using state machine)
	stateMachineName: string
	
	// Selected animation name (if using direct animation)
	animationName: string
	
	// Playback mode: state-machine, animation, or static
	playbackMode: object
	
	// How the Rive content fits the output
	fit: object
	
	// Alignment within the output bounds
	alignment: object
	
	// Background color (transparent if undefined)
	backgroundColor: string
}
``` 

## RiveProject

A Rive animation project imported into rship
### Queries

- [GetRiveProjects](#getriveprojects)
- [GetRiveProjectsByProjectId](#getriveprojectsbyprojectid)
- [GetRiveProjectById](#getriveprojectbyid)

```ts
{
	// Project this Rive animation belongs to
	// Belongs to Project
	projectId: object
	
	// Display name
	name: string
	
	// Description
	description: string
	
	// Asset ID in asset-store (S3/MinIO)
	assetId: string
	
	// Original filename
	filename: string
	
	// File size in bytes
	fileSize: number
	
	// File hash for cache invalidation
	fileHash: string
	
	// Extracted artboard metadata (JSON)
	artboardsJson: string
	
	// Extracted state machine metadata (JSON)
	stateMachinesJson: string
	
	// Extracted animation metadata (JSON)
	animationsJson: string
	
	// Thumbnail preview (base64 PNG)
	thumbnail: string
	
	// Created timestamp
	createdAt: number
	
	// Updated timestamp
	updatedAt: number
}
``` 

## Role

A permission role granting a user access to a specific entity
### Queries

- [GetRoles](#getroles)
- [GetRoleById](#getrolebyid)
- [GetRolesByUser](#getrolesbyuser)
- [GetRolesByEntity](#getrolesbyentity)
- [GetUserRoleForEntity](#getuserroleforentity)
- [GetRolesByOrganization](#getrolesbyorganization)

```ts
{
	// User who has this role
	userId: object
	
	// Type of entity: organization, network, machine, project, scene, calendar
	entityType: object
	
	// ID of the entity this role grants access to
	entityId: object
	
	// Permission level: viewer, manager, owner
	roleType: object
	
	// When this role was granted
	grantedAt: string
	
	// User who granted this role
	grantedBy: object
	
	// Optional expiration timestamp for temporary access
	expiresAt: string
	
	// Optional note about why this role was granted
	note: string
}
``` 

## Scene

A Scene that can be instantiated on a Calendar. Holds programmed data for Targets, Bindings between Emitters and Actions, and Event Tracks
### Queries

- [GetScenes](#getscenes)
- [GetScenesByQuery](#getscenesbyquery)
- [GetScenesByIds](#getscenesbyids)
- [GetSourceScenesForVariable](#getsourcescenesforvariable)
- [GetConsumerScenesForVariable](#getconsumerscenesforvariable)
- [GetScenesForVariable](#getscenesforvariable)

```ts
{
	// Searchable
	// Defaults to Main Scene
	name: string
	
	// Belongs to Project
	// Ensured for Project
	scopeId: object
	
	// ISO Duration string
	minDuration: string
	
	// ISO Duration string
	maxDuration: string
}
``` 

## Screen


### Queries

- [GetScreens](#getscreens)
- [GetScreensByProjectId](#getscreensbyprojectid)
- [GetScreenById](#getscreenbyid)
- [GetScreensByType](#getscreensbytype)
- [GetScreensByIds](#getscreensbyids)

```ts
{
	// Screen name
	name: string
	
	// Screen description
	description: string
	
	// Belongs to Project
	projectId: object
	
	// Screen type
	type: object
	
	// X position in meters
	x: number
	
	// Y position in meters
	y: number
	
	// Z position in meters
	z: number
	
	// X rotation in degrees
	rotX: number
	
	// Y rotation in degrees
	rotY: number
	
	// Z rotation in degrees
	rotZ: number
	
	// Physical width in meters
	width: number
	
	// Physical height in meters
	height: number
	
	// Output resolution width in pixels
	outputWidth: number
	
	// Output resolution height in pixels
	outputHeight: number
	
	// Refresh rate in Hz
	refreshRate: number
	
	// Color space (rec709, rec2020, dci-p3)
	colorSpace: string
	
	// Bit depth (8, 10, 12)
	bitDepth: number
	
	// Pixel pitch in millimeters (LED walls only)
	pixelPitch: number
	
	// LED mode - RGB or RGBW (LED walls only)
	ledMode: object
	
	// DMX universe (LED walls only)
	dmxUniverse: number
	
	// DMX start address (LED walls only)
	dmxAddress: number
	
	// Color correction enabled
	colorCorrectionEnabled: boolean
	
	// Color correction LUT file ID
	colorCorrectionLutId: object
	
	// Brightness (0-1, default 1)
	brightness: number
	
	// Contrast correction (0-2, default 1)
	contrastCorrection: number
	
	// Gamma correction (0.5-3, default 2.2)
	gammaCorrection: number
	
	// Physical output identifier (e.g., "HDMI-1", "SDI-2")
	outputIdentifier: string
	
	// Output enabled/disabled
	enabled: boolean
	
	// Test pattern enabled
	testPattern: boolean
	
	// Test pattern type
	testPatternType: string
	
	// Screen order in project
	order: number
	
	// Linked physical display ID (from Tauri display enumeration)
	physicalDisplayId: string
	
	// NVIDIA display ID for NVIDIA-specific operations
	nvidiaDisplayId: string
	
	// Physical display name (cached for display when disconnected)
	physicalDisplayName: string
	
	// Physical display connection type (cached)
	physicalDisplayConnection: string
	
	// Link/sync status
	linkStatus: object
	
	// Last sync error message
	lastSyncError: string
	
	// Last sync timestamp
	lastSyncAt: number
	
	// Set as primary display when syncing
	setPrimary: boolean
	
	// Position X in desktop coordinate space (for physical display)
	desktopX: number
	
	// Position Y in desktop coordinate space (for physical display)
	desktopY: number
	
	// Content stream ID (for video texture)
	streamId: object
	
	// Preview/testing color
	color: string
	
	// URL to imported 3D geometry in Asset Store
	geometryUrl: string
	
	// Render mode: imported, procedural, or hybrid
	renderMode: object
}
``` 

## Sequence


### Queries

- [GetSequences](#getsequences)
- [GetSequencesByQuery](#getsequencesbyquery)
- [GetSequencesByIds](#getsequencesbyids)

```ts
{
	// Belongs to Project
	projectId: object
	
	name: string
	
	description: string
	
	options: object
}
``` 

## SequencePlayback


### Queries

- [GetSequencePlaybacks](#getsequenceplaybacks)
- [GetSequencePlaybacksByQuery](#getsequenceplaybacksbyquery)
- [GetSequencePlaybacksByIds](#getsequenceplaybacksbyids)

```ts
{
	// Ensured for Session
	sessionId: object
	
	// Ensured for Sequence
	sequenceId: object
	
	// Defaults to none
	loopMode: object
}
``` 

## Server

A Myko Server 
### Queries

- [GetConnectedServer](#getconnectedserver)
- [GetPeerServers](#getpeerservers)
- [GetServersByClientIds](#getserversbyclientids)
- [GetServersByQuery](#getserversbyquery)
- [GetServers](#getservers)

```ts
{
	version: string
	
	// xxx.xxx.xxx.xxx, where it can be reached publically
	address: string
	
	// The port the server is listening on
	port: number
	
	// ISO DateTime string
	startedAt: string
}
``` 

## Session

The statefull instance of a project
### Queries

- [GetSessions](#getsessions)
- [GetSessionsByIds](#getsessionsbyids)
- [GetSessionsByScopeId](#getsessionsbyscopeid)

```ts
{
	// Defaults to Main Session
	name: string
	
	// Defaults to #0096c7
	color: object
	
	// Ensured for Project
	// Belongs to Project
	scopeId: object
	
	// The calendar that is used for autoplay in this session
	// Defaults to null
	calendarId: object
	
	// The scene that is used as the overview scene for this session
	// Defaults to null
	overviewSceneId: object
}
``` 

## SessionVariable


### Queries

- [GetSessionVariables](#getsessionvariables)
- [GetSessionVariablesByQuery](#getsessionvariablesbyquery)
- [GetSessionVariablesByIds](#getsessionvariablesbyids)

```ts
{
	// Belongs to Project
	scopeId: object
}
``` 

## SessionVariableValue


### Queries

- [GetSessionVariableValues](#getsessionvariablevalues)
- [GetSessionVariableValuesByQuery](#getsessionvariablevaluesbyquery)
- [GetSessionVariableValuesByIds](#getsessionvariablevaluesbyids)

```ts
{
	// Ensured for SessionVariable
	// Belongs to SessionVariable
	sessionVariableId: object
	
	// Ensured for Session
	// Belongs to Session
	sessionId: object
	
	// Defaults to undefined
	value: object
}
``` 

## Space

A physical or virtual space occupied by a subset of targets. Incomplete
### Queries

- [GetSpaces](#getspaces)
- [GetSpacesByIds](#getspacesbyids)

```ts
{
	// Belongs to Project
	scopeId: object
	
	// WARNING: deprecated
	serviceIds: string[]
}
``` 

## Stream


### Queries

- [GetStreams](#getstreams)
- [GetStreamsByQuery](#getstreamsbyquery)
- [GetStreamsByIds](#getstreamsbyids)

```ts
{
	// Belongs to Client
	// Auto Client ID
	clientId: object
}
``` 

## Sync


### Queries

- [GetSyncs](#getsyncs)
- [GetSyncsByQuery](#getsyncsbyquery)
- [GetSyncsByIds](#getsyncsbyids)

```ts
{
	
}
``` 

## Tag

A tag that can be applied to any entity
### Queries

- [GetTags](#gettags)
- [GetTagsByIds](#gettagsbyids)
- [GetTagsByQuery](#gettagsbyquery)
- [GetAssignedTags](#getassignedtags)

```ts
{
	// Belongs to Project
	scopeId: object
	
	name: string
}
``` 

## TagAssign


### Queries

- [GetTagAssigns](#gettagassigns)
- [GetTagAssignsByQuery](#gettagassignsbyquery)

```ts
{
	// Belongs to Tag
	tagId: object
	
	// Entity ID to which the tag is assigned
	entityId: object
	
	// Entity type to which the tag is assigned
	entityType: string
}
``` 

## Target

a logical unit owned by executors, housed in instances, that host actions and emitters
### Queries

- [GetTargets](#gettargets)
- [GetTargetsByIds](#gettargetsbyids)
- [GetTargetsByServiceId](#gettargetsbyserviceid)
- [GetRootLevelTargets](#getrootleveltargets)
- [GetOnlineRootLevelTargets](#getonlinerootleveltargets)
- [GetTargetsByParentTargetId](#gettargetsbyparenttargetid)
- [GetOnlineTargetsByParentId](#getonlinetargetsbyparentid)
- [GetTargetsByEmitterId](#gettargetsbyemitterid)
- [TargetsForInstance](#targetsforinstance)

```ts
{
	// Searchable
	name: string
	
	parentTargets: string[]
	
	// Searchable
	serviceId: object
	
	// Searchable
	category: string
	
	rootLevel: boolean
}
``` 

## TargetStatus

A Target's status for an instance
### Queries

- [GetTargetStatuses](#gettargetstatuses)
- [GetTargetStatusesByQuery](#gettargetstatusesbyquery)
- [GetTargetStatusesByIds](#gettargetstatusesbyids)
- [GetOnlineTargetStatusesBySession](#getonlinetargetstatusesbysession)

```ts
{
	// Belongs to Target
	targetId: object
	
	// Belongs to Instance
	instanceId: object
	
	status: 'online' | 'offline'
}
``` 

## Track


### Queries

- [GetTracks](#gettracks)
- [GetTracksByProjectId](#gettracksbyprojectid)
- [GetTrackById](#gettrackbyid)

```ts
{
	// Track name
	name: string
	
	// Track description
	description: string
	
	// Belongs to Project
	projectId: object
	
	// Current timecode position in milliseconds
	timecode: number
	
	// Playback state
	playing: boolean
	
	// Playback mode
	playbackMode: object
	
	// BPM (beats per minute) for music sync
	bpm: number
	
	// Track enabled/disabled
	enabled: boolean
	
	// Track order in timeline
	order: number
}
``` 

## User

A user account with profile information and organization membership
### Queries

- [GetUsers](#getusers)
- [GetUsersByIds](#getusersbyids)
- [GetUserById](#getuserbyid)
- [GetUserByAuth0Id](#getuserbyauth0id)
- [GetUserByEmail](#getuserbyemail)
- [GetUsersByOrganization](#getusersbyorganization)

```ts
{
	// Auth0 user ID (sub claim)
	auth0Id: string
	
	// Primary email address
	email: string
	
	// Whether email is verified
	emailVerified: boolean
	
	// Display name
	name: string
	
	// First name
	firstName: string
	
	// Last name
	lastName: string
	
	// Avatar/profile picture URL
	avatarUrl: string
	
	// User bio or description
	bio: string
	
	// Phone number
	phone: string
	
	// Primary organization ID (default context)
	primaryOrganizationId: object
	
	// Account status: active, suspended, pending, deactivated
	status: object
	
	// Last login timestamp
	lastLoginAt: string
	
	// Account creation timestamp
	createdAt: string
	
	// Preferred timezone (IANA)
	timezone: string
	
	// Preferred locale/language
	locale: string
	
	// UI theme preference
	theme: string
	
	// API key for programmatic access (hashed)
	apiKeyHash: string
	
	// API key prefix for identification
	apiKeyPrefix: string
	
	// When API key was last used
	apiKeyLastUsed: string
}
``` 

## VFXConnection

Connection between VFX nodes
### Queries

- [GetVFXConnectionsByGraphId](#getvfxconnectionsbygraphid)

```ts
{
	// Graph this connection belongs to
	// Belongs to VFXGraph
	graphId: object
	
	// Source node ID
	sourceNodeId: string
	
	// Source port ID
	sourcePortId: string
	
	// Target node ID
	targetNodeId: string
	
	// Target port ID
	targetPortId: string
}
``` 

## VFXGraph

A visual effects graph for real-time shader composition using Three.js TSL
### Queries

- [GetVFXGraphs](#getvfxgraphs)
- [GetVFXGraphsByProjectId](#getvfxgraphsbyprojectid)
- [GetVFXGraphById](#getvfxgraphbyid)

```ts
{
	// Project this graph belongs to
	// Belongs to Project
	projectId: object
	
	// Graph name
	name: string
	
	// Graph description
	description: string
	
	// Serialized graph data (nodes, connections)
	graphData: string
	
	// Output resolution width in pixels
	outputWidth: number
	
	// Output resolution height in pixels
	outputHeight: number
	
	// Target frame rate
	targetFps: number
	
	// Graph color for UI
	color: string
	
	// Graph is enabled
	enabled: boolean
	
	// Graph thumbnail (base64 encoded)
	thumbnail: string
	
	// Last compilation error (if any)
	compilationError: string
	
	// Last successful compilation timestamp
	lastCompiledAt: number
}
``` 

## VFXGraphLayer

A VFX graph layer in the timeline - provides generative content to the video pipeline
### Queries

- [GetVFXGraphLayers](#getvfxgraphlayers)
- [GetVFXGraphLayersByTrackId](#getvfxgraphlayersbytrackid)
- [GetVFXGraphLayerById](#getvfxgraphlayerbyid)
- [GetVFXGraphLayersByVFXGraphId](#getvfxgraphlayersbyvfxgraphid)

```ts
{
	// Layer type (always "vfx-graph")
	type: string
	
	// Layer name
	name: string
	
	// Layer description
	description: string
	
	// Track this layer belongs to
	// Belongs to Track
	trackId: object
	
	// VFX graph to render
	// Belongs to VFXGraph
	vfxGraphId: object
	
	// Mapping for output routing
	// Belongs to Mapping
	mappingId: object
	
	// Layer order in track (higher = on top)
	order: number
	
	// Layer opacity (0-1)
	opacity: number
	
	// Blend mode for compositing
	blendMode: object
	
	// Layer enabled/disabled
	enabled: boolean
	
	// Layer solo (only render this layer)
	solo: boolean
	
	// Layer locked (prevent editing)
	locked: boolean
	
	// Start time in milliseconds
	startTime: number
	
	// Duration in milliseconds (undefined = infinite)
	duration: number
	
	// Layer color for UI
	color: string
	
	// Input bindings for sampling from other layers (JSON serialized)
	inputBindings: string
	
	// Time offset in milliseconds
	timeOffset: number
	
	// Playback speed multiplier
	playbackSpeed: number
	
	// Loop the VFX graph animation
	loop: boolean
	
	// Override output resolution (use graph default if undefined)
	overrideWidth: number
}
``` 

## VFXNodePosition

Position and state of a VFX node in the graph editor
### Queries

- [GetVFXNodePositionsByGraphId](#getvfxnodepositionsbygraphid)

```ts
{
	// Graph this node belongs to
	// Belongs to VFXGraph
	graphId: object
	
	// Node ID within the graph
	nodeId: string
	
	// X position in editor
	x: number
	
	// Y position in editor
	y: number
	
	// Width in editor
	w: number
	
	// Height in editor
	h: number
	
	// Node is collapsed
	collapsed: boolean
}
``` 

## VFXNodeValue

Property value for a VFX node
### Queries

- [GetVFXNodeValuesByGraphId](#getvfxnodevaluesbygraphid)
- [GetVFXNodeValue](#getvfxnodevalue)

```ts
{
	// Graph this value belongs to
	// Belongs to VFXGraph
	graphId: object
	
	// Node ID within the graph
	nodeId: string
	
	// Property ID
	propertyId: string
	
	// Property value (JSON serialized)
	value: string
}
``` 

## VideoLayer


### Queries

- [GetVideoLayers](#getvideolayers)
- [GetVideoLayersByTrackId](#getvideolayersbytrackid)
- [GetVideoLayerById](#getvideolayerbyid)
- [GetVideoLayersByStreamId](#getvideolayersbystreamid)

```ts
{
	// Layer type (always "video")
	type: string
	
	// Layer name
	name: string
	
	// Layer description
	description: string
	
	// Belongs to Track
	trackId: object
	
	// Belongs to Stream
	streamId: object
	
	// Belongs to Mapping
	mappingId: object
	
	// Layer order in track (higher = on top)
	order: number
	
	// Layer opacity (0-1)
	opacity: number
	
	// Blend mode for compositing
	blendMode: string
	
	// Layer enabled/disabled
	enabled: boolean
	
	// Layer solo (only render this layer)
	solo: boolean
	
	// Layer locked (prevent editing)
	locked: boolean
	
	// Start time in milliseconds
	startTime: number
	
	// Duration in milliseconds (undefined = infinite)
	duration: number
	
	// Layer color for UI
	color: string
	
	// Playback speed multiplier (1.0 = normal)
	playbackSpeed: number
	
	// Playback mode
	playbackMode: object
	
	// End behavior when video reaches end
	endBehavior: object
	
	// Video in-point in milliseconds
	inPoint: number
	
	// Video out-point in milliseconds
	outPoint: number
	
	// Video brightness adjustment (-1 to 1)
	brightness: number
	
	// Video contrast adjustment (-1 to 1)
	contrast: number
	
	// Video saturation adjustment (-1 to 1)
	saturation: number
	
	// Video hue rotation in degrees (0-360)
	hue: number
	
	// Flip horizontal
	flipHorizontal: boolean
	
	// Flip vertical
	flipVertical: boolean
}
``` 

## WindowGroup

A group of windows that track the same session
### Queries

- [GetWindowGroups](#getwindowgroups)
- [GetWindowGroupsById](#getwindowgroupsbyid)
- [GetWindowGroupsByOwnerId](#getwindowgroupsbyownerid)

```ts
{
	// Ensured for User
	ownerId: object
	
	projectId: object
	
	sessionIdByProject: object
	
	// Defaults to 
	selectedTargets: array
	
	// Defaults to 
	selectedBundles: array
	
	// Defaults to 
	selectedScreens: array
	
	// Defaults to null
	paneLayout: object
}
``` 

# Queries

## GetEventLog

Returns: [EventContainer[]](#eventcontainer)
```ts
{
	queryId: 'GetEventLog' 
	{
		tx: String
		commandClientId: String
	}
}
``` 

## GetLogs

Returns: [Log[]](#log)
```ts
{
	queryId: 'GetLogs' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetConnectedServer

Returns: [Server[]](#server)
```ts
{
	queryId: 'GetConnectedServer' 
	{
		tx: String
	}
}
``` 

## GetPeerServers

Returns: [Server[]](#server)
```ts
{
	queryId: 'GetPeerServers' 
	{
		tx: String
	}
}
``` 

## GetServersByClientIds

Returns: [Server[]](#server)
```ts
{
	queryId: 'GetServersByClientIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetServersByQuery

Returns: [Server[]](#server)
```ts
{
	queryId: 'GetServersByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetServers

Returns: [Server[]](#server)
```ts
{
	queryId: 'GetServers' 
	{
		tx: String
	}
}
``` 

## GetClientsByIds

Returns: [Client[]](#client)
```ts
{
	queryId: 'GetClientsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetClientsByQuery

Returns: [Client[]](#client)
```ts
{
	queryId: 'GetClientsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## PeerQuery


```ts
{
	queryId: 'PeerQuery' 
	{
		tx: String
		commandClientId: MQuery
		createdAt: Object
	}
}
``` 

## EventsForEntity

Returns: [EventContainer[]](#eventcontainer)
```ts
{
	queryId: 'EventsForEntity' 
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: String
	}
}
``` 

## GetTargets

Returns: [Target[]](#target)
```ts
{
	queryId: 'GetTargets' 
	{
		tx: String
	}
}
``` 

## GetTargetsByIds

Returns: [Target[]](#target)
```ts
{
	queryId: 'GetTargetsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetTargetsByServiceId

Returns: [Target[]](#target)
```ts
{
	queryId: 'GetTargetsByServiceId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetRootLevelTargets

Returns: [Target[]](#target)
```ts
{
	queryId: 'GetRootLevelTargets' 
	{
		tx: String
	}
}
``` 

## GetOnlineRootLevelTargets

Returns: [Target[]](#target)
```ts
{
	queryId: 'GetOnlineRootLevelTargets' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetTargetsByParentTargetId

Returns: [Target[]](#target)
```ts
{
	queryId: 'GetTargetsByParentTargetId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetOnlineTargetsByParentId

Returns: [Target[]](#target)
```ts
{
	queryId: 'GetOnlineTargetsByParentId' 
	{
		tx: String
		commandClientId: Object
		createdAt: Object
	}
}
``` 

## GetTargetsByEmitterId

Returns: [Target[]](#target)
```ts
{
	queryId: 'GetTargetsByEmitterId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## TargetsForInstance

Returns: [Target[]](#target)
```ts
{
	queryId: 'TargetsForInstance' 
	{
		tx: String
		commandClientId: Object
		createdAt: Object
	}
}
``` 

## GetActions

Returns: [Action[]](#action)
```ts
{
	queryId: 'GetActions' 
	{
		tx: String
	}
}
``` 

## GetActionsByIds

Returns: [Action[]](#action)
```ts
{
	queryId: 'GetActionsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetActionsByQuery

Returns: [Action[]](#action)
```ts
{
	queryId: 'GetActionsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetProjects

Returns: [Project[]](#project)
```ts
{
	queryId: 'GetProjects' 
	{
		tx: String
	}
}
``` 

## GetProjectsByIds

Returns: [Project[]](#project)
```ts
{
	queryId: 'GetProjectsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetScenes

Returns: [Scene[]](#scene)
```ts
{
	queryId: 'GetScenes' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetScenesByQuery

Returns: [Scene[]](#scene)
```ts
{
	queryId: 'GetScenesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetScenesByIds

Returns: [Scene[]](#scene)
```ts
{
	queryId: 'GetScenesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetSessions

Returns: [Session[]](#session)
```ts
{
	queryId: 'GetSessions' 
	{
		tx: String
	}
}
``` 

## GetSessionsByIds

Returns: [Session[]](#session)
```ts
{
	queryId: 'GetSessionsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetSessionsByScopeId

Returns: [Session[]](#session)
```ts
{
	queryId: 'GetSessionsByScopeId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetActiveScenes

Returns: [ActiveScene[]](#activescene)
```ts
{
	queryId: 'GetActiveScenes' 
	{
		tx: String
	}
}
``` 

## GetActiveScenesByQuery

Returns: [ActiveScene[]](#activescene)
```ts
{
	queryId: 'GetActiveScenesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetActiveScenesByIds

Returns: [ActiveScene[]](#activescene)
```ts
{
	queryId: 'GetActiveScenesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetInstances

Returns: [Instance[]](#instance)
```ts
{
	queryId: 'GetInstances' 
	{
		tx: String
	}
}
``` 

## GetInstancesByQuery

Returns: [Instance[]](#instance)
```ts
{
	queryId: 'GetInstancesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetInstancesByIds

Returns: [Instance[]](#instance)
```ts
{
	queryId: 'GetInstancesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetAlerts

Returns: [Alert[]](#alert)
```ts
{
	queryId: 'GetAlerts' 
	{
		tx: String
	}
}
``` 

## GetAlertsByQuery

Returns: [Alert[]](#alert)
```ts
{
	queryId: 'GetAlertsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetAlertsByIds

Returns: [Alert[]](#alert)
```ts
{
	queryId: 'GetAlertsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetAlertsByEntities

Returns: [Alert[]](#alert)
```ts
{
	queryId: 'GetAlertsByEntities' 
	{
		tx: String
		commandClientId: Array
		createdAt: Object
	}
}
``` 

## GetAuditLogs

Returns: [AuditLog[]](#auditlog)
```ts
{
	queryId: 'GetAuditLogs' 
	{
		tx: String
	}
}
``` 

## GetAuditLogsByOrganization

Returns: [AuditLog[]](#auditlog)
```ts
{
	queryId: 'GetAuditLogsByOrganization' 
	{
		tx: String
		commandClientId: Object
		createdAt: Object
	}
}
``` 

## GetAuditLogsByUser

Returns: [AuditLog[]](#auditlog)
```ts
{
	queryId: 'GetAuditLogsByUser' 
	{
		tx: String
		commandClientId: Object
		createdAt: Object
	}
}
``` 

## GetAuditLogsByTarget

Returns: [AuditLog[]](#auditlog)
```ts
{
	queryId: 'GetAuditLogsByTarget' 
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SearchAuditLogs

Returns: [AuditLog[]](#auditlog)
```ts
{
	queryId: 'SearchAuditLogs' 
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## GetTracks

Returns: [Track[]](#track)
```ts
{
	queryId: 'GetTracks' 
	{
		tx: String
	}
}
``` 

## GetTracksByProjectId

Returns: [Track[]](#track)
```ts
{
	queryId: 'GetTracksByProjectId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetTrackById

Returns: [Track[]](#track)
```ts
{
	queryId: 'GetTrackById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetCompositions

Returns: [Composition[]](#composition)
```ts
{
	queryId: 'GetCompositions' 
	{
		tx: String
	}
}
``` 

## GetCompositionsByProjectId

Returns: [Composition[]](#composition)
```ts
{
	queryId: 'GetCompositionsByProjectId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetCompositionById

Returns: [Composition[]](#composition)
```ts
{
	queryId: 'GetCompositionById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetCompositionsByIds

Returns: [Composition[]](#composition)
```ts
{
	queryId: 'GetCompositionsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetCalendars

Returns: [Calendar[]](#calendar)
```ts
{
	queryId: 'GetCalendars' 
	{
		tx: String
	}
}
``` 

## GetCalendarsByQuery

Returns: [Calendar[]](#calendar)
```ts
{
	queryId: 'GetCalendarsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetCalendarsByIds

Returns: [Calendar[]](#calendar)
```ts
{
	queryId: 'GetCalendarsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetAppearances

Returns: [Appearance[]](#appearance)
```ts
{
	queryId: 'GetAppearances' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetAppearancesByIds

Returns: [Appearance[]](#appearance)
```ts
{
	queryId: 'GetAppearancesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetAppearancesByQuery

Returns: [Appearance[]](#appearance)
```ts
{
	queryId: 'GetAppearancesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetAppearancesBySpaceId

Returns: [Appearance[]](#appearance)
```ts
{
	queryId: 'GetAppearancesBySpaceId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetAssets

Returns: [Asset[]](#asset)
```ts
{
	queryId: 'GetAssets' 
	{
		tx: String
	}
}
``` 

## GetAssetsByIds

Returns: [Asset[]](#asset)
```ts
{
	queryId: 'GetAssetsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetPayloads

Returns: [Payload[]](#payload)
```ts
{
	queryId: 'GetPayloads' 
	{
		tx: String
	}
}
``` 

## GetPayloadsByIds

Returns: [Payload[]](#payload)
```ts
{
	queryId: 'GetPayloadsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetPayloadsByServiceIds

Returns: [Payload[]](#payload)
```ts
{
	queryId: 'GetPayloadsByServiceIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetBundles

Returns: [Bundle[]](#bundle)
```ts
{
	queryId: 'GetBundles' 
	{
		tx: String
	}
}
``` 

## GetBundlesByScopeId

Returns: [Bundle[]](#bundle)
```ts
{
	queryId: 'GetBundlesByScopeId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetBundlesByIds

Returns: [Bundle[]](#bundle)
```ts
{
	queryId: 'GetBundlesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetEmitters

Returns: [Emitter[]](#emitter)
```ts
{
	queryId: 'GetEmitters' 
	{
		tx: String
	}
}
``` 

## GetEmittersByIds

Returns: [Emitter[]](#emitter)
```ts
{
	queryId: 'GetEmittersByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetEmittersByQuery

Returns: [Emitter[]](#emitter)
```ts
{
	queryId: 'GetEmittersByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetBindings

Returns: [Binding[]](#binding)
```ts
{
	queryId: 'GetBindings' 
	{
		tx: String
	}
}
``` 

## GetBindingsByIds

Returns: [Binding[]](#binding)
```ts
{
	queryId: 'GetBindingsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetBindingsByEmitterId

Returns: [Binding[]](#binding)
```ts
{
	queryId: 'GetBindingsByEmitterId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetBindingsByScopeId

Returns: [Binding[]](#binding)
```ts
{
	queryId: 'GetBindingsByScopeId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetEventTracks

Returns: [EventTrack[]](#eventtrack)
```ts
{
	queryId: 'GetEventTracks' 
	{
		tx: String
	}
}
``` 

## GetEventTracksByQuery

Returns: [EventTrack[]](#eventtrack)
```ts
{
	queryId: 'GetEventTracksByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetEventTracksByIds

Returns: [EventTrack[]](#eventtrack)
```ts
{
	queryId: 'GetEventTracksByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetStartEventTracksByScene

Returns: [EventTrack[]](#eventtrack)
```ts
{
	queryId: 'GetStartEventTracksByScene' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetEndEventTracksByScene

Returns: [EventTrack[]](#eventtrack)
```ts
{
	queryId: 'GetEndEventTracksByScene' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetGlobalVariables

Returns: [GlobalVariable[]](#globalvariable)
```ts
{
	queryId: 'GetGlobalVariables' 
	{
		tx: String
	}
}
``` 

## GetGlobalVariablesByQuery

Returns: [GlobalVariable[]](#globalvariable)
```ts
{
	queryId: 'GetGlobalVariablesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetGlobalVariablesByIds

Returns: [GlobalVariable[]](#globalvariable)
```ts
{
	queryId: 'GetGlobalVariablesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetSourceScenesForVariable

Returns: [Scene[]](#scene)
```ts
{
	queryId: 'GetSourceScenesForVariable' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetConsumerScenesForVariable

Returns: [Scene[]](#scene)
```ts
{
	queryId: 'GetConsumerScenesForVariable' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetScenesForVariable

Returns: [Scene[]](#scene)
```ts
{
	queryId: 'GetScenesForVariable' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetGlobalVariablesSetByScene

Returns: [GlobalVariable[]](#globalvariable)
```ts
{
	queryId: 'GetGlobalVariablesSetByScene' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetGlobalVariablesConsumedByScene

Returns: [GlobalVariable[]](#globalvariable)
```ts
{
	queryId: 'GetGlobalVariablesConsumedByScene' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetGlobalVariablesInScene

Returns: [GlobalVariable[]](#globalvariable)
```ts
{
	queryId: 'GetGlobalVariablesInScene' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetBindingNodes

Returns: [BindingNode[]](#bindingnode)
```ts
{
	queryId: 'GetBindingNodes' 
	{
		tx: String
	}
}
``` 

## GetBindingNodesByQuery

Returns: [BindingNode[]](#bindingnode)
```ts
{
	queryId: 'GetBindingNodesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetBindingNodesByIds

Returns: [BindingNode[]](#bindingnode)
```ts
{
	queryId: 'GetBindingNodesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetBindingNodesInputConnectedToType

Returns: [BindingNode[]](#bindingnode)
```ts
{
	queryId: 'GetBindingNodesInputConnectedToType' 
	{
		tx: String
		commandClientId: Object
		createdAt: String
		lineage: Object
	}
}
``` 

## GetEventTrackPlaybacks

Returns: [EventTrackPlayback[]](#eventtrackplayback)
```ts
{
	queryId: 'GetEventTrackPlaybacks' 
	{
		tx: String
	}
}
``` 

## GetEventTrackPlaybacksByQuery

Returns: [EventTrackPlayback[]](#eventtrackplayback)
```ts
{
	queryId: 'GetEventTrackPlaybacksByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetEventTrackPlaybacksByIds

Returns: [EventTrackPlayback[]](#eventtrackplayback)
```ts
{
	queryId: 'GetEventTrackPlaybacksByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetCameras

Returns: [Camera[]](#camera)
```ts
{
	queryId: 'GetCameras' 
	{
		tx: String
	}
}
``` 

## GetCamerasByQuery

Returns: [Camera[]](#camera)
```ts
{
	queryId: 'GetCamerasByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetCamerasByIds

Returns: [Camera[]](#camera)
```ts
{
	queryId: 'GetCamerasByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetCalibrations

Returns: [Calibration[]](#calibration)
```ts
{
	queryId: 'GetCalibrations' 
	{
		tx: String
	}
}
``` 

## GetCalibrationsByQuery

Returns: [Calibration[]](#calibration)
```ts
{
	queryId: 'GetCalibrationsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetCalibrationsByIds

Returns: [Calibration[]](#calibration)
```ts
{
	queryId: 'GetCalibrationsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetSessionVariables

Returns: [SessionVariable[]](#sessionvariable)
```ts
{
	queryId: 'GetSessionVariables' 
	{
		tx: String
	}
}
``` 

## GetSessionVariablesByQuery

Returns: [SessionVariable[]](#sessionvariable)
```ts
{
	queryId: 'GetSessionVariablesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetSessionVariablesByIds

Returns: [SessionVariable[]](#sessionvariable)
```ts
{
	queryId: 'GetSessionVariablesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetSessionVariableValues

Returns: [SessionVariableValue[]](#sessionvariablevalue)
```ts
{
	queryId: 'GetSessionVariableValues' 
	{
		tx: String
	}
}
``` 

## GetSessionVariableValuesByQuery

Returns: [SessionVariableValue[]](#sessionvariablevalue)
```ts
{
	queryId: 'GetSessionVariableValuesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetSessionVariableValuesByIds

Returns: [SessionVariableValue[]](#sessionvariablevalue)
```ts
{
	queryId: 'GetSessionVariableValuesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetBindingNodeConnections

Returns: [BindingNodeConnection[]](#bindingnodeconnection)
```ts
{
	queryId: 'GetBindingNodeConnections' 
	{
		tx: String
	}
}
``` 

## GetBindingNodeConnectionsByQuery

Returns: [BindingNodeConnection[]](#bindingnodeconnection)
```ts
{
	queryId: 'GetBindingNodeConnectionsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetBindingNodeConnectionsByIds

Returns: [BindingNodeConnection[]](#bindingnodeconnection)
```ts
{
	queryId: 'GetBindingNodeConnectionsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetBindingNodePositions

Returns: [BindingNodePosition[]](#bindingnodeposition)
```ts
{
	queryId: 'GetBindingNodePositions' 
	{
		tx: String
	}
}
``` 

## GetBindingNodePositionsByQuery

Returns: [BindingNodePosition[]](#bindingnodeposition)
```ts
{
	queryId: 'GetBindingNodePositionsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetBindingNodePositionsByIds

Returns: [BindingNodePosition[]](#bindingnodeposition)
```ts
{
	queryId: 'GetBindingNodePositionsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetBindingNodeValues

Returns: [BindingNodeValue[]](#bindingnodevalue)
```ts
{
	queryId: 'GetBindingNodeValues' 
	{
		tx: String
	}
}
``` 

## GetBindingNodeValuesByQuery

Returns: [BindingNodeValue[]](#bindingnodevalue)
```ts
{
	queryId: 'GetBindingNodeValuesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetBundleStatuses

Returns: [BundleStatus[]](#bundlestatus)
```ts
{
	queryId: 'GetBundleStatuses' 
	{
		tx: String
	}
}
``` 

## GetBundleStatusesByQuery

Returns: [BundleStatus[]](#bundlestatus)
```ts
{
	queryId: 'GetBundleStatusesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetBundleStatusesByIds

Returns: [BundleStatus[]](#bundlestatus)
```ts
{
	queryId: 'GetBundleStatusesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetBundleStatusesByBundlesAndConfig

Returns: [BundleStatus[]](#bundlestatus)
```ts
{
	queryId: 'GetBundleStatusesByBundlesAndConfig' 
	{
		tx: String
		commandClientId: Array
		createdAt: Object
	}
}
``` 

## GetPoints

Returns: [Point[]](#point)
```ts
{
	queryId: 'GetPoints' 
	{
		tx: String
	}
}
``` 

## GetPointsByQuery

Returns: [Point[]](#point)
```ts
{
	queryId: 'GetPointsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetPointsByIds

Returns: [Point[]](#point)
```ts
{
	queryId: 'GetPointsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetCalibrationPoints

Returns: [CalibrationPoint[]](#calibrationpoint)
```ts
{
	queryId: 'GetCalibrationPoints' 
	{
		tx: String
	}
}
``` 

## GetCalibrationPointsByQuery

Returns: [CalibrationPoint[]](#calibrationpoint)
```ts
{
	queryId: 'GetCalibrationPointsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetCalibrationPointsByIds

Returns: [CalibrationPoint[]](#calibrationpoint)
```ts
{
	queryId: 'GetCalibrationPointsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetClusterTargetAssigns

Returns: [ClusterTargetAssign[]](#clustertargetassign)
```ts
{
	queryId: 'GetClusterTargetAssigns' 
	{
		tx: String
	}
}
``` 

## GetClusterTargetAssignsByQuery

Returns: [ClusterTargetAssign[]](#clustertargetassign)
```ts
{
	queryId: 'GetClusterTargetAssignsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetClusterTargetAssignsByIds

Returns: [ClusterTargetAssign[]](#clustertargetassign)
```ts
{
	queryId: 'GetClusterTargetAssignsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetColorProfiles

Returns: [ColorProfile[]](#colorprofile)
```ts
{
	queryId: 'GetColorProfiles' 
	{
		tx: String
	}
}
``` 

## GetColorProfilesByQuery

Returns: [ColorProfile[]](#colorprofile)
```ts
{
	queryId: 'GetColorProfilesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetColorProfilesByIds

Returns: [ColorProfile[]](#colorprofile)
```ts
{
	queryId: 'GetColorProfilesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetConstraints

Returns: [Constraint[]](#constraint)
```ts
{
	queryId: 'GetConstraints' 
	{
		tx: String
	}
}
``` 

## GetConstraintsByIds

Returns: [Constraint[]](#constraint)
```ts
{
	queryId: 'GetConstraintsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetConstraintsByQuery

Returns: [Constraint[]](#constraint)
```ts
{
	queryId: 'GetConstraintsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetSequences

Returns: [Sequence[]](#sequence)
```ts
{
	queryId: 'GetSequences' 
	{
		tx: String
	}
}
``` 

## GetSequencesByQuery

Returns: [Sequence[]](#sequence)
```ts
{
	queryId: 'GetSequencesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetSequencesByIds

Returns: [Sequence[]](#sequence)
```ts
{
	queryId: 'GetSequencesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetCues

Returns: [Cue[]](#cue)
```ts
{
	queryId: 'GetCues' 
	{
		tx: String
	}
}
``` 

## GetCuesByQuery

Returns: [Cue[]](#cue)
```ts
{
	queryId: 'GetCuesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetCuesByIds

Returns: [Cue[]](#cue)
```ts
{
	queryId: 'GetCuesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetCuePlaybacks

Returns: [CuePlayback[]](#cueplayback)
```ts
{
	queryId: 'GetCuePlaybacks' 
	{
		tx: String
	}
}
``` 

## GetCuePlaybacksByQuery

Returns: [CuePlayback[]](#cueplayback)
```ts
{
	queryId: 'GetCuePlaybacksByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetCuePlaybacksByIds

Returns: [CuePlayback[]](#cueplayback)
```ts
{
	queryId: 'GetCuePlaybacksByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetCuePlaybacksByCueIds

Returns: [CuePlayback[]](#cueplayback)
```ts
{
	queryId: 'GetCuePlaybacksByCueIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetCurves

Returns: [Curve[]](#curve)
```ts
{
	queryId: 'GetCurves' 
	{
		tx: String
	}
}
``` 

## GetCurvesByQuery

Returns: [Curve[]](#curve)
```ts
{
	queryId: 'GetCurvesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetCurvesByIds

Returns: [Curve[]](#curve)
```ts
{
	queryId: 'GetCurvesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetDataTransfers

Returns: [DataTransfer[]](#datatransfer)
```ts
{
	queryId: 'GetDataTransfers' 
	{
		tx: String
	}
}
``` 

## GetDataTransfersByQuery

Returns: [DataTransfer[]](#datatransfer)
```ts
{
	queryId: 'GetDataTransfersByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetDataTransfersByIds

Returns: [DataTransfer[]](#datatransfer)
```ts
{
	queryId: 'GetDataTransfersByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetDeviceOwnership

Returns: [DeviceOwnership[]](#deviceownership)
```ts
{
	queryId: 'GetDeviceOwnership' 
	{
		tx: String
	}
}
``` 

## GetDeviceOwnershipByMachine

Returns: [DeviceOwnership[]](#deviceownership)
```ts
{
	queryId: 'GetDeviceOwnershipByMachine' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetDeviceOwnershipsByOrganization

Returns: [DeviceOwnership[]](#deviceownership)
```ts
{
	queryId: 'GetDeviceOwnershipsByOrganization' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetDeviceOwnershipsByUser

Returns: [DeviceOwnership[]](#deviceownership)
```ts
{
	queryId: 'GetDeviceOwnershipsByUser' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetUnclaimedDevices

Returns: [DeviceOwnership[]](#deviceownership)
```ts
{
	queryId: 'GetUnclaimedDevices' 
	{
		tx: String
	}
}
``` 

## GetPendingClaims

Returns: [DeviceOwnership[]](#deviceownership)
```ts
{
	queryId: 'GetPendingClaims' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetDeviceShares

Returns: [DeviceShare[]](#deviceshare)
```ts
{
	queryId: 'GetDeviceShares' 
	{
		tx: String
	}
}
``` 

## GetDeviceSharesByMachine

Returns: [DeviceShare[]](#deviceshare)
```ts
{
	queryId: 'GetDeviceSharesByMachine' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetDeviceSharesByUser

Returns: [DeviceShare[]](#deviceshare)
```ts
{
	queryId: 'GetDeviceSharesByUser' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetDirectories

Returns: [Directory[]](#directory)
```ts
{
	queryId: 'GetDirectories' 
	{
		tx: String
	}
}
``` 

## GetDirectoriesByIds

Returns: [Directory[]](#directory)
```ts
{
	queryId: 'GetDirectoriesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetEventTrackLayers

Returns: [EventTrackLayer[]](#eventtracklayer)
```ts
{
	queryId: 'GetEventTrackLayers' 
	{
		tx: String
	}
}
``` 

## GetEventTrackLayersByQuery

Returns: [EventTrackLayer[]](#eventtracklayer)
```ts
{
	queryId: 'GetEventTrackLayersByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetEventTrackLayersByIds

Returns: [EventTrackLayer[]](#eventtracklayer)
```ts
{
	queryId: 'GetEventTrackLayersByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetMachines

Returns: [Machine[]](#machine)
```ts
{
	queryId: 'GetMachines' 
	{
		tx: String
	}
}
``` 

## GetMachinesByQuery

Returns: [Machine[]](#machine)
```ts
{
	queryId: 'GetMachinesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetMachinesByIds

Returns: [Machine[]](#machine)
```ts
{
	queryId: 'GetMachinesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetMachineById

Returns: [Machine[]](#machine)
```ts
{
	queryId: 'GetMachineById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetMachineByJnOtpAccountName

Returns: [Machine[]](#machine)
```ts
{
	queryId: 'GetMachineByJnOtpAccountName' 
	{
		tx: String
		commandClientId: String
	}
}
``` 

## GetMachineBySerialNumber

Returns: [Machine[]](#machine)
```ts
{
	queryId: 'GetMachineBySerialNumber' 
	{
		tx: String
		commandClientId: String
	}
}
``` 

## GetExecLogs

Returns: [ExecLog[]](#execlog)
```ts
{
	queryId: 'GetExecLogs' 
	{
		tx: String
	}
}
``` 

## GetExecLogsByQuery

Returns: [ExecLog[]](#execlog)
```ts
{
	queryId: 'GetExecLogsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetExecLogsByIds

Returns: [ExecLog[]](#execlog)
```ts
{
	queryId: 'GetExecLogsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetFeeds

Returns: [Feed[]](#feed)
```ts
{
	queryId: 'GetFeeds' 
	{
		tx: String
	}
}
``` 

## GetFeedsByIds

Returns: [Feed[]](#feed)
```ts
{
	queryId: 'GetFeedsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetFiles

Returns: [File[]](#file)
```ts
{
	queryId: 'GetFiles' 
	{
		tx: String
	}
}
``` 

## GetFilesByIds

Returns: [File[]](#file)
```ts
{
	queryId: 'GetFilesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetFilesByMachineId

Returns: [File[]](#file)
```ts
{
	queryId: 'GetFilesByMachineId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## AvailableFilesForQuery

Returns: [File[]](#file)
```ts
{
	queryId: 'AvailableFilesForQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetFileTransfers

Returns: [FileTransfer[]](#filetransfer)
```ts
{
	queryId: 'GetFileTransfers' 
	{
		tx: String
	}
}
``` 

## GetFileTransfersByQuery

Returns: [FileTransfer[]](#filetransfer)
```ts
{
	queryId: 'GetFileTransfersByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetFileTransfersByIds

Returns: [FileTransfer[]](#filetransfer)
```ts
{
	queryId: 'GetFileTransfersByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetDMXUniverses

Returns: [DMXUniverse[]](#dmxuniverse)
```ts
{
	queryId: 'GetDMXUniverses' 
	{
		tx: String
	}
}
``` 

## GetDMXUniversesByQuery

Returns: [DMXUniverse[]](#dmxuniverse)
```ts
{
	queryId: 'GetDMXUniversesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetDMXUniversesByIds

Returns: [DMXUniverse[]](#dmxuniverse)
```ts
{
	queryId: 'GetDMXUniversesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetDMXUniversesByProjectId

Returns: [DMXUniverse[]](#dmxuniverse)
```ts
{
	queryId: 'GetDMXUniversesByProjectId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetDMXUniverseByNumber

Returns: [DMXUniverse[]](#dmxuniverse)
```ts
{
	queryId: 'GetDMXUniverseByNumber' 
	{
		tx: String
		commandClientId: Object
		createdAt: Number
	}
}
``` 

## GetFixtures

Returns: [Fixture[]](#fixture)
```ts
{
	queryId: 'GetFixtures' 
	{
		tx: String
	}
}
``` 

## GetFixturesByQuery

Returns: [Fixture[]](#fixture)
```ts
{
	queryId: 'GetFixturesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetFixturesByIds

Returns: [Fixture[]](#fixture)
```ts
{
	queryId: 'GetFixturesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetFixturesByProjectId

Returns: [Fixture[]](#fixture)
```ts
{
	queryId: 'GetFixturesByProjectId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetFixturesByFixtureTypeId

Returns: [Fixture[]](#fixture)
```ts
{
	queryId: 'GetFixturesByFixtureTypeId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetFixtureTypes

Returns: [FixtureType[]](#fixturetype)
```ts
{
	queryId: 'GetFixtureTypes' 
	{
		tx: String
	}
}
``` 

## GetFixtureTypesByQuery

Returns: [FixtureType[]](#fixturetype)
```ts
{
	queryId: 'GetFixtureTypesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetFixtureTypesByIds

Returns: [FixtureType[]](#fixturetype)
```ts
{
	queryId: 'GetFixtureTypesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetFixtureCalibrations

Returns: [FixtureCalibration[]](#fixturecalibration)
```ts
{
	queryId: 'GetFixtureCalibrations' 
	{
		tx: String
	}
}
``` 

## GetFixtureCalibrationsByQuery

Returns: [FixtureCalibration[]](#fixturecalibration)
```ts
{
	queryId: 'GetFixtureCalibrationsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetFixtureCalibrationsByIds

Returns: [FixtureCalibration[]](#fixturecalibration)
```ts
{
	queryId: 'GetFixtureCalibrationsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetFixtureGroups

Returns: [FixtureGroup[]](#fixturegroup)
```ts
{
	queryId: 'GetFixtureGroups' 
	{
		tx: String
	}
}
``` 

## GetFixtureGroupsByQuery

Returns: [FixtureGroup[]](#fixturegroup)
```ts
{
	queryId: 'GetFixtureGroupsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetFixtureGroupsByIds

Returns: [FixtureGroup[]](#fixturegroup)
```ts
{
	queryId: 'GetFixtureGroupsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetFixtureGroupsByProjectId

Returns: [FixtureGroup[]](#fixturegroup)
```ts
{
	queryId: 'GetFixtureGroupsByProjectId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetFixtureGroupsByFixtureId

Returns: [FixtureGroup[]](#fixturegroup)
```ts
{
	queryId: 'GetFixtureGroupsByFixtureId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetFixturePresets

Returns: [FixturePreset[]](#fixturepreset)
```ts
{
	queryId: 'GetFixturePresets' 
	{
		tx: String
	}
}
``` 

## GetFixturePresetsByQuery

Returns: [FixturePreset[]](#fixturepreset)
```ts
{
	queryId: 'GetFixturePresetsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetFixturePresetsByIds

Returns: [FixturePreset[]](#fixturepreset)
```ts
{
	queryId: 'GetFixturePresetsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetFixturePresetsByProjectId

Returns: [FixturePreset[]](#fixturepreset)
```ts
{
	queryId: 'GetFixturePresetsByProjectId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetInstanceAssigns

Returns: [InstanceAssign[]](#instanceassign)
```ts
{
	queryId: 'GetInstanceAssigns' 
	{
		tx: String
	}
}
``` 

## GetInstanceAssignsByIds

Returns: [InstanceAssign[]](#instanceassign)
```ts
{
	queryId: 'GetInstanceAssignsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetInstanceAssignsByQuery

Returns: [InstanceAssign[]](#instanceassign)
```ts
{
	queryId: 'GetInstanceAssignsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetInvites

Returns: [Invite[]](#invite)
```ts
{
	queryId: 'GetInvites' 
	{
		tx: String
	}
}
``` 

## GetInviteByCode

Returns: [Invite[]](#invite)
```ts
{
	queryId: 'GetInviteByCode' 
	{
		tx: String
		commandClientId: String
	}
}
``` 

## GetInvitesByEntity

Returns: [Invite[]](#invite)
```ts
{
	queryId: 'GetInvitesByEntity' 
	{
		tx: String
		commandClientId: Object
		createdAt: Object
	}
}
``` 

## GetInvitesByEmail

Returns: [Invite[]](#invite)
```ts
{
	queryId: 'GetInvitesByEmail' 
	{
		tx: String
		commandClientId: String
	}
}
``` 

## GetPendingInvitesByOrganization

Returns: [Invite[]](#invite)
```ts
{
	queryId: 'GetPendingInvitesByOrganization' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetInstanceClusterAssigns

Returns: [InstanceClusterAssign[]](#instanceclusterassign)
```ts
{
	queryId: 'GetInstanceClusterAssigns' 
	{
		tx: String
	}
}
``` 

## GetInstanceClusterAssignsByQuery

Returns: [InstanceClusterAssign[]](#instanceclusterassign)
```ts
{
	queryId: 'GetInstanceClusterAssignsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetInstanceClusterAssignsByIds

Returns: [InstanceClusterAssign[]](#instanceclusterassign)
```ts
{
	queryId: 'GetInstanceClusterAssignsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetKeyframes

Returns: [Keyframe[]](#keyframe)
```ts
{
	queryId: 'GetKeyframes' 
	{
		tx: String
	}
}
``` 

## GetKeyframesByQuery

Returns: [Keyframe[]](#keyframe)
```ts
{
	queryId: 'GetKeyframesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetKeyframesByIds

Returns: [Keyframe[]](#keyframe)
```ts
{
	queryId: 'GetKeyframesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetMappings

Returns: [Mapping[]](#mapping)
```ts
{
	queryId: 'GetMappings' 
	{
		tx: String
	}
}
``` 

## GetMappingsByProjectId

Returns: [Mapping[]](#mapping)
```ts
{
	queryId: 'GetMappingsByProjectId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetMappingById

Returns: [Mapping[]](#mapping)
```ts
{
	queryId: 'GetMappingById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetMappingsByType

Returns: [Mapping[]](#mapping)
```ts
{
	queryId: 'GetMappingsByType' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetMappingsByScreenId

Returns: [Mapping[]](#mapping)
```ts
{
	queryId: 'GetMappingsByScreenId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetLayers

Returns: [Layer[]](#layer)
```ts
{
	queryId: 'GetLayers' 
	{
		tx: String
	}
}
``` 

## GetLayersByTrackId

Returns: [Layer[]](#layer)
```ts
{
	queryId: 'GetLayersByTrackId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetLayerById

Returns: [Layer[]](#layer)
```ts
{
	queryId: 'GetLayerById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetLayerGroups

Returns: [LayerGroup[]](#layergroup)
```ts
{
	queryId: 'GetLayerGroups' 
	{
		tx: String
	}
}
``` 

## GetLayerGroupsByCompositionId

Returns: [LayerGroup[]](#layergroup)
```ts
{
	queryId: 'GetLayerGroupsByCompositionId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetLayerGroupById

Returns: [LayerGroup[]](#layergroup)
```ts
{
	queryId: 'GetLayerGroupById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetLayerGroupsByIds

Returns: [LayerGroup[]](#layergroup)
```ts
{
	queryId: 'GetLayerGroupsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetLEDWalls

Returns: [LEDWall[]](#ledwall)
```ts
{
	queryId: 'GetLEDWalls' 
	{
		tx: String
	}
}
``` 

## GetLEDWallsByProjectId

Returns: [LEDWall[]](#ledwall)
```ts
{
	queryId: 'GetLEDWallsByProjectId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetLEDWallsByIds

Returns: [LEDWall[]](#ledwall)
```ts
{
	queryId: 'GetLEDWallsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetLensFiles

Returns: [LensFile[]](#lensfile)
```ts
{
	queryId: 'GetLensFiles' 
	{
		tx: String
	}
}
``` 

## GetLensFilesByQuery

Returns: [LensFile[]](#lensfile)
```ts
{
	queryId: 'GetLensFilesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetLensFilesByIds

Returns: [LensFile[]](#lensfile)
```ts
{
	queryId: 'GetLensFilesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetLinkExecs

Returns: [LinkExec[]](#linkexec)
```ts
{
	queryId: 'GetLinkExecs' 
	{
		tx: String
	}
}
``` 

## GetLinkExecsByQuery

Returns: [LinkExec[]](#linkexec)
```ts
{
	queryId: 'GetLinkExecsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetLinkExecsByIds

Returns: [LinkExec[]](#linkexec)
```ts
{
	queryId: 'GetLinkExecsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetLinkExecRunners

Returns: [LinkExecRunner[]](#linkexecrunner)
```ts
{
	queryId: 'GetLinkExecRunners' 
	{
		tx: String
	}
}
``` 

## GetLinkExecRunnersByQuery

Returns: [LinkExecRunner[]](#linkexecrunner)
```ts
{
	queryId: 'GetLinkExecRunnersByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetLinkExecRunnersByIds

Returns: [LinkExecRunner[]](#linkexecrunner)
```ts
{
	queryId: 'GetLinkExecRunnersByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetLinkLogs

Returns: [LinkLog[]](#linklog)
```ts
{
	queryId: 'GetLinkLogs' 
	{
		tx: String
	}
}
``` 

## GetLinkLogsByQuery

Returns: [LinkLog[]](#linklog)
```ts
{
	queryId: 'GetLinkLogsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetLinkLogsByIds

Returns: [LinkLog[]](#linklog)
```ts
{
	queryId: 'GetLinkLogsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetLocations

Returns: [Location[]](#location)
```ts
{
	queryId: 'GetLocations' 
	{
		tx: String
	}
}
``` 

## GetLocationsByQuery

Returns: [Location[]](#location)
```ts
{
	queryId: 'GetLocationsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetLocationsByIds

Returns: [Location[]](#location)
```ts
{
	queryId: 'GetLocationsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetMachineStatuses

Returns: [MachineStatus[]](#machinestatus)
```ts
{
	queryId: 'GetMachineStatuses' 
	{
		tx: String
	}
}
``` 

## GetMachineStatusesByQuery

Returns: [MachineStatus[]](#machinestatus)
```ts
{
	queryId: 'GetMachineStatusesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetMachineStatusesByIds

Returns: [MachineStatus[]](#machinestatus)
```ts
{
	queryId: 'GetMachineStatusesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetMachineStatusesByMachineId

Returns: [MachineStatus[]](#machinestatus)
```ts
{
	queryId: 'GetMachineStatusesByMachineId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetMeasurements

Returns: [Measurement[]](#measurement)
```ts
{
	queryId: 'GetMeasurements' 
	{
		tx: String
	}
}
``` 

## GetMeasurementsByProjectId

Returns: [Measurement[]](#measurement)
```ts
{
	queryId: 'GetMeasurementsByProjectId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetMeasurementsByIds

Returns: [Measurement[]](#measurement)
```ts
{
	queryId: 'GetMeasurementsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetNetworks

Returns: [Network[]](#network)
```ts
{
	queryId: 'GetNetworks' 
	{
		tx: String
	}
}
``` 

## GetNetworkById

Returns: [Network[]](#network)
```ts
{
	queryId: 'GetNetworkById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetNetworksByOrganization

Returns: [Network[]](#network)
```ts
{
	queryId: 'GetNetworksByOrganization' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetNetworkByVpnId

Returns: [Network[]](#network)
```ts
{
	queryId: 'GetNetworkByVpnId' 
	{
		tx: String
		commandClientId: String
	}
}
``` 

## GetNetworkMembers

Returns: [NetworkMember[]](#networkmember)
```ts
{
	queryId: 'GetNetworkMembers' 
	{
		tx: String
	}
}
``` 

## GetNetworkMembersByNetwork

Returns: [NetworkMember[]](#networkmember)
```ts
{
	queryId: 'GetNetworkMembersByNetwork' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetNetworkMembersByMachine

Returns: [NetworkMember[]](#networkmember)
```ts
{
	queryId: 'GetNetworkMembersByMachine' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetNetworkMember

Returns: [NetworkMember[]](#networkmember)
```ts
{
	queryId: 'GetNetworkMember' 
	{
		tx: String
		commandClientId: Object
		createdAt: Object
	}
}
``` 

## GetOrganizations

Returns: [Organization[]](#organization)
```ts
{
	queryId: 'GetOrganizations' 
	{
		tx: String
	}
}
``` 

## GetOrganizationById

Returns: [Organization[]](#organization)
```ts
{
	queryId: 'GetOrganizationById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetOrganizationBySlug

Returns: [Organization[]](#organization)
```ts
{
	queryId: 'GetOrganizationBySlug' 
	{
		tx: String
		commandClientId: String
	}
}
``` 

## GetOrganizationsByUser

Returns: [Organization[]](#organization)
```ts
{
	queryId: 'GetOrganizationsByUser' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetOverviewNodes

Returns: [OverviewNode[]](#overviewnode)
```ts
{
	queryId: 'GetOverviewNodes' 
	{
		tx: String
	}
}
``` 

## GetOverviewNodesByQuery

Returns: [OverviewNode[]](#overviewnode)
```ts
{
	queryId: 'GetOverviewNodesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetOverviewNodesByIds

Returns: [OverviewNode[]](#overviewnode)
```ts
{
	queryId: 'GetOverviewNodesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetUsers

Returns: [User[]](#user)
```ts
{
	queryId: 'GetUsers' 
	{
		tx: String
	}
}
``` 

## GetUsersByIds

Returns: [User[]](#user)
```ts
{
	queryId: 'GetUsersByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetUserById

Returns: [User[]](#user)
```ts
{
	queryId: 'GetUserById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetUserByAuth0Id

Returns: [User[]](#user)
```ts
{
	queryId: 'GetUserByAuth0Id' 
	{
		tx: String
		commandClientId: String
	}
}
``` 

## GetUserByEmail

Returns: [User[]](#user)
```ts
{
	queryId: 'GetUserByEmail' 
	{
		tx: String
		commandClientId: String
	}
}
``` 

## GetUsersByOrganization

Returns: [User[]](#user)
```ts
{
	queryId: 'GetUsersByOrganization' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetPanes

Returns: [Pane[]](#pane)
```ts
{
	queryId: 'GetPanes' 
	{
		tx: String
	}
}
``` 

## GetPanesByIds

Returns: [Pane[]](#pane)
```ts
{
	queryId: 'GetPanesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetPanesByWindowGroupId

Returns: [Pane[]](#pane)
```ts
{
	queryId: 'GetPanesByWindowGroupId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetPanesByOwnerId

Returns: [Pane[]](#pane)
```ts
{
	queryId: 'GetPanesByOwnerId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetPorts

Returns: [Port[]](#port)
```ts
{
	queryId: 'GetPorts' 
	{
		tx: String
	}
}
``` 

## GetPortsByMachine

Returns: [Port[]](#port)
```ts
{
	queryId: 'GetPortsByMachine' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetPortById

Returns: [Port[]](#port)
```ts
{
	queryId: 'GetPortById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetPortsByNetwork

Returns: [Port[]](#port)
```ts
{
	queryId: 'GetPortsByNetwork' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetPayloadStatuss

Returns: [PayloadStatus[]](#payloadstatus)
```ts
{
	queryId: 'GetPayloadStatuss' 
	{
		tx: String
	}
}
``` 

## GetPayloadStatussByQuery

Returns: [PayloadStatus[]](#payloadstatus)
```ts
{
	queryId: 'GetPayloadStatussByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetPayloadStatussByIds

Returns: [PayloadStatus[]](#payloadstatus)
```ts
{
	queryId: 'GetPayloadStatussByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetArmedPayloadsForInstances

Returns: [Payload[]](#payload)
```ts
{
	queryId: 'GetArmedPayloadsForInstances' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetPlaylists

Returns: [Playlist[]](#playlist)
```ts
{
	queryId: 'GetPlaylists' 
	{
		tx: String
	}
}
``` 

## GetPlaylistsByQuery

Returns: [Playlist[]](#playlist)
```ts
{
	queryId: 'GetPlaylistsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetPlaylistsByIds

Returns: [Playlist[]](#playlist)
```ts
{
	queryId: 'GetPlaylistsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetPlaylistAppearances

Returns: [PlaylistAppearance[]](#playlistappearance)
```ts
{
	queryId: 'GetPlaylistAppearances' 
	{
		tx: String
	}
}
``` 

## GetPlaylistAppearancesByQuery

Returns: [PlaylistAppearance[]](#playlistappearance)
```ts
{
	queryId: 'GetPlaylistAppearancesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetPlaylistAppearancesByIds

Returns: [PlaylistAppearance[]](#playlistappearance)
```ts
{
	queryId: 'GetPlaylistAppearancesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetPlaylistItems

Returns: [PlaylistItem[]](#playlistitem)
```ts
{
	queryId: 'GetPlaylistItems' 
	{
		tx: String
	}
}
``` 

## GetPlaylistItemsByQuery

Returns: [PlaylistItem[]](#playlistitem)
```ts
{
	queryId: 'GetPlaylistItemsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetPlaylistItemsByIds

Returns: [PlaylistItem[]](#playlistitem)
```ts
{
	queryId: 'GetPlaylistItemsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetProps

Returns: [Prop[]](#prop)
```ts
{
	queryId: 'GetProps' 
	{
		tx: String
	}
}
``` 

## GetPropsByQuery

Returns: [Prop[]](#prop)
```ts
{
	queryId: 'GetPropsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetPropsByProjectId

Returns: [Prop[]](#prop)
```ts
{
	queryId: 'GetPropsByProjectId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetPropsByIds

Returns: [Prop[]](#prop)
```ts
{
	queryId: 'GetPropsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetPulsesByIds

Returns: [Pulse[]](#pulse)
```ts
{
	queryId: 'GetPulsesByIds' 
	{
		tx: String
		commandClientId: Array
		createdAt: Object
	}
}
``` 

## GetPulsesByEmitterId

Returns: [Pulse[]](#pulse)
```ts
{
	queryId: 'GetPulsesByEmitterId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetResolutionStrategys

Returns: [ResolutionStrategy[]](#resolutionstrategy)
```ts
{
	queryId: 'GetResolutionStrategys' 
	{
		tx: String
	}
}
``` 

## GetResolutionStrategysByQuery

Returns: [ResolutionStrategy[]](#resolutionstrategy)
```ts
{
	queryId: 'GetResolutionStrategysByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetResolutionStrategysByIds

Returns: [ResolutionStrategy[]](#resolutionstrategy)
```ts
{
	queryId: 'GetResolutionStrategysByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetRoles

Returns: [Role[]](#role)
```ts
{
	queryId: 'GetRoles' 
	{
		tx: String
	}
}
``` 

## GetRoleById

Returns: [Role[]](#role)
```ts
{
	queryId: 'GetRoleById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetRolesByUser

Returns: [Role[]](#role)
```ts
{
	queryId: 'GetRolesByUser' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetRolesByEntity

Returns: [Role[]](#role)
```ts
{
	queryId: 'GetRolesByEntity' 
	{
		tx: String
		commandClientId: Object
		createdAt: Object
	}
}
``` 

## GetUserRoleForEntity

Returns: [Role[]](#role)
```ts
{
	queryId: 'GetUserRoleForEntity' 
	{
		tx: String
		commandClientId: Object
		createdAt: Object
		lineage: Object
	}
}
``` 

## GetRolesByOrganization

Returns: [Role[]](#role)
```ts
{
	queryId: 'GetRolesByOrganization' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetScreens

Returns: [Screen[]](#screen)
```ts
{
	queryId: 'GetScreens' 
	{
		tx: String
	}
}
``` 

## GetScreensByProjectId

Returns: [Screen[]](#screen)
```ts
{
	queryId: 'GetScreensByProjectId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetScreenById

Returns: [Screen[]](#screen)
```ts
{
	queryId: 'GetScreenById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetScreensByType

Returns: [Screen[]](#screen)
```ts
{
	queryId: 'GetScreensByType' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetScreensByIds

Returns: [Screen[]](#screen)
```ts
{
	queryId: 'GetScreensByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetSequencePlaybacks

Returns: [SequencePlayback[]](#sequenceplayback)
```ts
{
	queryId: 'GetSequencePlaybacks' 
	{
		tx: String
	}
}
``` 

## GetSequencePlaybacksByQuery

Returns: [SequencePlayback[]](#sequenceplayback)
```ts
{
	queryId: 'GetSequencePlaybacksByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetSequencePlaybacksByIds

Returns: [SequencePlayback[]](#sequenceplayback)
```ts
{
	queryId: 'GetSequencePlaybacksByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetSpaces

Returns: [Space[]](#space)
```ts
{
	queryId: 'GetSpaces' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetSpacesByIds

Returns: [Space[]](#space)
```ts
{
	queryId: 'GetSpacesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetStreams

Returns: [Stream[]](#stream)
```ts
{
	queryId: 'GetStreams' 
	{
		tx: String
	}
}
``` 

## GetStreamsByQuery

Returns: [Stream[]](#stream)
```ts
{
	queryId: 'GetStreamsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetStreamsByIds

Returns: [Stream[]](#stream)
```ts
{
	queryId: 'GetStreamsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetSyncs

Returns: [Sync[]](#sync)
```ts
{
	queryId: 'GetSyncs' 
	{
		tx: String
	}
}
``` 

## GetSyncsByQuery

Returns: [Sync[]](#sync)
```ts
{
	queryId: 'GetSyncsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetSyncsByIds

Returns: [Sync[]](#sync)
```ts
{
	queryId: 'GetSyncsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetTags

Returns: [Tag[]](#tag)
```ts
{
	queryId: 'GetTags' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetTagsByIds

Returns: [Tag[]](#tag)
```ts
{
	queryId: 'GetTagsByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetTagsByQuery

Returns: [Tag[]](#tag)
```ts
{
	queryId: 'GetTagsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetAssignedTags

Returns: [Tag[]](#tag)
```ts
{
	queryId: 'GetAssignedTags' 
	{
		tx: String
		commandClientId: Object
		createdAt: String
	}
}
``` 

## GetTagAssigns

Returns: [TagAssign[]](#tagassign)
```ts
{
	queryId: 'GetTagAssigns' 
	{
		tx: String
	}
}
``` 

## GetTagAssignsByQuery

Returns: [TagAssign[]](#tagassign)
```ts
{
	queryId: 'GetTagAssignsByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetTargetStatuses

Returns: [TargetStatus[]](#targetstatus)
```ts
{
	queryId: 'GetTargetStatuses' 
	{
		tx: String
	}
}
``` 

## GetTargetStatusesByQuery

Returns: [TargetStatus[]](#targetstatus)
```ts
{
	queryId: 'GetTargetStatusesByQuery' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetTargetStatusesByIds

Returns: [TargetStatus[]](#targetstatus)
```ts
{
	queryId: 'GetTargetStatusesByIds' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetOnlineTargetStatusesBySession

Returns: [TargetStatus[]](#targetstatus)
```ts
{
	queryId: 'GetOnlineTargetStatusesBySession' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetVideoLayers

Returns: [VideoLayer[]](#videolayer)
```ts
{
	queryId: 'GetVideoLayers' 
	{
		tx: String
	}
}
``` 

## GetVideoLayersByTrackId

Returns: [VideoLayer[]](#videolayer)
```ts
{
	queryId: 'GetVideoLayersByTrackId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetVideoLayerById

Returns: [VideoLayer[]](#videolayer)
```ts
{
	queryId: 'GetVideoLayerById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetVideoLayersByStreamId

Returns: [VideoLayer[]](#videolayer)
```ts
{
	queryId: 'GetVideoLayersByStreamId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetVFXGraphs

Returns: [VFXGraph[]](#vfxgraph)
```ts
{
	queryId: 'GetVFXGraphs' 
	{
		tx: String
	}
}
``` 

## GetVFXGraphsByProjectId

Returns: [VFXGraph[]](#vfxgraph)
```ts
{
	queryId: 'GetVFXGraphsByProjectId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetVFXGraphById

Returns: [VFXGraph[]](#vfxgraph)
```ts
{
	queryId: 'GetVFXGraphById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetVFXNodePositionsByGraphId

Returns: [VFXNodePosition[]](#vfxnodeposition)
```ts
{
	queryId: 'GetVFXNodePositionsByGraphId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetVFXNodeValuesByGraphId

Returns: [VFXNodeValue[]](#vfxnodevalue)
```ts
{
	queryId: 'GetVFXNodeValuesByGraphId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetVFXNodeValue

Returns: [VFXNodeValue[]](#vfxnodevalue)
```ts
{
	queryId: 'GetVFXNodeValue' 
	{
		tx: String
		commandClientId: Object
		createdAt: String
		lineage: String
	}
}
``` 

## GetVFXConnectionsByGraphId

Returns: [VFXConnection[]](#vfxconnection)
```ts
{
	queryId: 'GetVFXConnectionsByGraphId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetVFXGraphLayers

Returns: [VFXGraphLayer[]](#vfxgraphlayer)
```ts
{
	queryId: 'GetVFXGraphLayers' 
	{
		tx: String
	}
}
``` 

## GetVFXGraphLayersByTrackId

Returns: [VFXGraphLayer[]](#vfxgraphlayer)
```ts
{
	queryId: 'GetVFXGraphLayersByTrackId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetVFXGraphLayerById

Returns: [VFXGraphLayer[]](#vfxgraphlayer)
```ts
{
	queryId: 'GetVFXGraphLayerById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetVFXGraphLayersByVFXGraphId

Returns: [VFXGraphLayer[]](#vfxgraphlayer)
```ts
{
	queryId: 'GetVFXGraphLayersByVFXGraphId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetRenderClusters

Returns: [RenderCluster[]](#rendercluster)
```ts
{
	queryId: 'GetRenderClusters' 
	{
		tx: String
	}
}
``` 

## GetRenderClustersByProjectId

Returns: [RenderCluster[]](#rendercluster)
```ts
{
	queryId: 'GetRenderClustersByProjectId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetRenderClusterById

Returns: [RenderCluster[]](#rendercluster)
```ts
{
	queryId: 'GetRenderClusterById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetRenderClustersByStatus

Returns: [RenderCluster[]](#rendercluster)
```ts
{
	queryId: 'GetRenderClustersByStatus' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetRenderNodes

Returns: [RenderNode[]](#rendernode)
```ts
{
	queryId: 'GetRenderNodes' 
	{
		tx: String
	}
}
``` 

## GetRenderNodesByClusterId

Returns: [RenderNode[]](#rendernode)
```ts
{
	queryId: 'GetRenderNodesByClusterId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetRenderNodeById

Returns: [RenderNode[]](#rendernode)
```ts
{
	queryId: 'GetRenderNodeById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetRenderNodesByStatus

Returns: [RenderNode[]](#rendernode)
```ts
{
	queryId: 'GetRenderNodesByStatus' 
	{
		tx: String
		commandClientId: Object
		createdAt: Object
	}
}
``` 

## GetRenderNodeByBrowserFingerprint

Returns: [RenderNode[]](#rendernode)
```ts
{
	queryId: 'GetRenderNodeByBrowserFingerprint' 
	{
		tx: String
		commandClientId: Object
		createdAt: String
	}
}
``` 

## GetRiveProjects

Returns: [RiveProject[]](#riveproject)
```ts
{
	queryId: 'GetRiveProjects' 
	{
		tx: String
	}
}
``` 

## GetRiveProjectsByProjectId

Returns: [RiveProject[]](#riveproject)
```ts
{
	queryId: 'GetRiveProjectsByProjectId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetRiveProjectById

Returns: [RiveProject[]](#riveproject)
```ts
{
	queryId: 'GetRiveProjectById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetRiveLayers

Returns: [RiveLayer[]](#rivelayer)
```ts
{
	queryId: 'GetRiveLayers' 
	{
		tx: String
	}
}
``` 

## GetRiveLayersByTrackId

Returns: [RiveLayer[]](#rivelayer)
```ts
{
	queryId: 'GetRiveLayersByTrackId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetRiveLayerById

Returns: [RiveLayer[]](#rivelayer)
```ts
{
	queryId: 'GetRiveLayerById' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetRiveLayersByProjectId

Returns: [RiveLayer[]](#rivelayer)
```ts
{
	queryId: 'GetRiveLayersByProjectId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

## GetWindowGroups

Returns: [WindowGroup[]](#windowgroup)
```ts
{
	queryId: 'GetWindowGroups' 
	{
		tx: String
	}
}
``` 

## GetWindowGroupsById

Returns: [WindowGroup[]](#windowgroup)
```ts
{
	queryId: 'GetWindowGroupsById' 
	{
		tx: String
		commandClientId: Array
	}
}
``` 

## GetWindowGroupsByOwnerId

Returns: [WindowGroup[]](#windowgroup)
```ts
{
	queryId: 'GetWindowGroupsByOwnerId' 
	{
		tx: String
		commandClientId: Object
	}
}
``` 

# Commands

## SetLogLevel

```ts
{
	commandId: 'SetLogLevel'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## ReballanceItem

```ts
{
	commandId: 'ReballanceItem'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
	}
}
``` 

## DeleteClientsByServerId

```ts
{
	commandId: 'DeleteClientsByServerId'
	{
		tx: String
		commandClientId: String
		createdAt: String
	}
}
``` 

## ClientCommand

```ts
{
	commandId: 'ClientCommand'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: withType
	}
}
``` 

## SetClientWindbackTime

```ts
{
	commandId: 'SetClientWindbackTime'
	{
		tx: String
		commandClientId: String
		createdAt: String
	}
}
``` 

## ClearClientWindbackTime

```ts
{
	commandId: 'ClearClientWindbackTime'
	{
		tx: String
		commandClientId: String
	}
}
``` 

## PeerCommand

```ts
{
	commandId: 'PeerCommand'
	{
		tx: String
		commandClientId: String
		createdAt: MCommand
		lineage: Object
	}
}
``` 

## ImportItems

```ts
{
	commandId: 'ImportItems'
	{
		tx: String
		commandClientId: String
		createdAt: Array
	}
}
``` 

## CreateProject

```ts
{
	commandId: 'CreateProject'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: String
	}
}
``` 

## SaveProject

```ts
{
	commandId: 'SaveProject'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: String
		$commandResult: String
	}
}
``` 

## DeleteProject

```ts
{
	commandId: 'DeleteProject'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreateScene

```ts
{
	commandId: 'CreateScene'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
		$commandResult: Object
		userToken: Array
	}
}
``` 

## RenameScene

```ts
{
	commandId: 'RenameScene'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SceneSetMinDuration

```ts
{
	commandId: 'SceneSetMinDuration'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SceneSetMaxDuration

```ts
{
	commandId: 'SceneSetMaxDuration'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## DeleteScene

```ts
{
	commandId: 'DeleteScene'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## AssignNewBundleToScene

```ts
{
	commandId: 'AssignNewBundleToScene'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## AssignBundleToScene

```ts
{
	commandId: 'AssignBundleToScene'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## RemoveBundleFromScene

```ts
{
	commandId: 'RemoveBundleFromScene'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CloneScene

```ts
{
	commandId: 'CloneScene'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreateSession

```ts
{
	commandId: 'CreateSession'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
		$commandResult: Object
	}
}
``` 

## SetSessionColor

```ts
{
	commandId: 'SetSessionColor'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetSessionName

```ts
{
	commandId: 'SetSessionName'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## DeleteSession

```ts
{
	commandId: 'DeleteSession'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SetCalendar

```ts
{
	commandId: 'SetCalendar'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CloneSession

```ts
{
	commandId: 'CloneSession'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SetOverviewScene

```ts
{
	commandId: 'SetOverviewScene'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## ArmScene

```ts
{
	commandId: 'ArmScene'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## ArmMany

```ts
{
	commandId: 'ArmMany'
	{
		tx: String
		commandClientId: String
		createdAt: Array
		lineage: Object
	}
}
``` 

## DisarmScene

```ts
{
	commandId: 'DisarmScene'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## DisarmMany

```ts
{
	commandId: 'DisarmMany'
	{
		tx: String
		commandClientId: String
		createdAt: Array
		lineage: Object
	}
}
``` 

## BuildOn

```ts
{
	commandId: 'BuildOn'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## BuildOnMany

```ts
{
	commandId: 'BuildOnMany'
	{
		tx: String
		commandClientId: String
		createdAt: Array
		lineage: Object
	}
}
``` 

## BuildOff

```ts
{
	commandId: 'BuildOff'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## BuildOffMany

```ts
{
	commandId: 'BuildOffMany'
	{
		tx: String
		commandClientId: String
		createdAt: Array
		lineage: Object
	}
}
``` 

## ReparentActiveScene

```ts
{
	commandId: 'ReparentActiveScene'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## EmergencyStopAll

```ts
{
	commandId: 'EmergencyStopAll'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## ActivateSafeState

```ts
{
	commandId: 'ActivateSafeState'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetInstancesOfflineByClient

```ts
{
	commandId: 'SetInstancesOfflineByClient'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## DeleteInstances

```ts
{
	commandId: 'DeleteInstances'
	{
		tx: String
		commandClientId: String
		createdAt: Array
	}
}
``` 

## MarkInstancesOffline

```ts
{
	commandId: 'MarkInstancesOffline'
	{
		tx: String
		commandClientId: String
		createdAt: Array
	}
}
``` 

## CreateAuditLog

```ts
{
	commandId: 'CreateAuditLog'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
	}
}
``` 

## CreateTrack

```ts
{
	commandId: 'CreateTrack'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
	}
}
``` 

## DeleteTrack

```ts
{
	commandId: 'DeleteTrack'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateTrack

```ts
{
	commandId: 'UpdateTrack'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetTrackTimecode

```ts
{
	commandId: 'SetTrackTimecode'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
	}
}
``` 

## PlayTrack

```ts
{
	commandId: 'PlayTrack'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## PauseTrack

```ts
{
	commandId: 'PauseTrack'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## StopTrack

```ts
{
	commandId: 'StopTrack'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreateComposition

```ts
{
	commandId: 'CreateComposition'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
	}
}
``` 

## DeleteComposition

```ts
{
	commandId: 'DeleteComposition'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateComposition

```ts
{
	commandId: 'UpdateComposition'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetCompositionOrder

```ts
{
	commandId: 'SetCompositionOrder'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
	}
}
``` 

## SetCompositionEnabled

```ts
{
	commandId: 'SetCompositionEnabled'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Boolean
	}
}
``` 

## CreateCalendar

```ts
{
	commandId: 'CreateCalendar'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
	}
}
``` 

## CreateAppearance

```ts
{
	commandId: 'CreateAppearance'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: String
		$commandResult: Object
		userToken: Object
		startTime: Object
	}
}
``` 

## DeleteAppearance

```ts
{
	commandId: 'DeleteAppearance'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## DeleteManyAppearances

```ts
{
	commandId: 'DeleteManyAppearances'
	{
		tx: String
		commandClientId: String
		createdAt: Array
	}
}
``` 

## SetAppearanceTimes

```ts
{
	commandId: 'SetAppearanceTimes'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: String
	}
}
``` 

## SavePayloadData

```ts
{
	commandId: 'SavePayloadData'
	{
		tx: String
		commandClientId: String
		createdAt: Array
	}
}
``` 

## CreatePayloadInBundle

```ts
{
	commandId: 'CreatePayloadInBundle'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SendPayload

```ts
{
	commandId: 'SendPayload'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## DeletePayload

```ts
{
	commandId: 'DeletePayload'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreateBundle

```ts
{
	commandId: 'CreateBundle'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## DeleteBundle

```ts
{
	commandId: 'DeleteBundle'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## AddPayload

```ts
{
	commandId: 'AddPayload'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## RemovePayload

```ts
{
	commandId: 'RemovePayload'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SendBundle

```ts
{
	commandId: 'SendBundle'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreateBinding

```ts
{
	commandId: 'CreateBinding'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## DeleteBinding

```ts
{
	commandId: 'DeleteBinding'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreateEventTrack

```ts
{
	commandId: 'CreateEventTrack'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
		$commandResult: Object
	}
}
``` 

## DeleteEventTrack

```ts
{
	commandId: 'DeleteEventTrack'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## RenameEventTrack

```ts
{
	commandId: 'RenameEventTrack'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetEventTrackLock

```ts
{
	commandId: 'SetEventTrackLock'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Boolean
	}
}
``` 

## SetEventTrackLockAll

```ts
{
	commandId: 'SetEventTrackLockAll'
	{
		tx: String
		commandClientId: String
		createdAt: Boolean
		lineage: Object
	}
}
``` 

## SetEventTrackTimeMode

```ts
{
	commandId: 'SetEventTrackTimeMode'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetSceneDeactivatedKey

```ts
{
	commandId: 'SetSceneDeactivatedKey'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Number
		userToken: Object
	}
}
``` 

## SetSceneActivatedKey

```ts
{
	commandId: 'SetSceneActivatedKey'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Number
		userToken: Object
	}
}
``` 

## CreateGlobalVariable

```ts
{
	commandId: 'CreateGlobalVariable'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
	}
}
``` 

## DeleteGlobalVariable

```ts
{
	commandId: 'DeleteGlobalVariable'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SetGlobalVariableResolutionStrategy

```ts
{
	commandId: 'SetGlobalVariableResolutionStrategy'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## LinkGlobalVariableResolutionStrategy

```ts
{
	commandId: 'LinkGlobalVariableResolutionStrategy'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## CloneGlobalVariableResolutionStrategy

```ts
{
	commandId: 'CloneGlobalVariableResolutionStrategy'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## RenameGlobalVariable

```ts
{
	commandId: 'RenameGlobalVariable'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## PromoteNodeOutputToGlobalVariable

```ts
{
	commandId: 'PromoteNodeOutputToGlobalVariable'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## PromoteNodeInputToGlobalVariable

```ts
{
	commandId: 'PromoteNodeInputToGlobalVariable'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreateBindingNode

```ts
{
	commandId: 'CreateBindingNode'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## SetBindingNodeEventTrack

```ts
{
	commandId: 'SetBindingNodeEventTrack'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetBindingNodeBuildOn

```ts
{
	commandId: 'SetBindingNodeBuildOn'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetBindingNodeBuildOff

```ts
{
	commandId: 'SetBindingNodeBuildOff'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetBindingNodeBundle

```ts
{
	commandId: 'SetBindingNodeBundle'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetBindingNodeSceneOutput

```ts
{
	commandId: 'SetBindingNodeSceneOutput'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetBindingNodeGlobalVariable

```ts
{
	commandId: 'SetBindingNodeGlobalVariable'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## DeleteBindingNode

```ts
{
	commandId: 'DeleteBindingNode'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## DeleteBindingNodes

```ts
{
	commandId: 'DeleteBindingNodes'
	{
		tx: String
		commandClientId: String
		createdAt: Array
	}
}
``` 

## RenameBindingNode

```ts
{
	commandId: 'RenameBindingNode'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## CloneBindingNodes

```ts
{
	commandId: 'CloneBindingNodes'
	{
		tx: String
		commandClientId: String
		createdAt: Array
		lineage: Object
	}
}
``` 

## ReplaceTargetBindingNodes

```ts
{
	commandId: 'ReplaceTargetBindingNodes'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
		userToken: Array
	}
}
``` 

## ForceExecuteBindingNode

```ts
{
	commandId: 'ForceExecuteBindingNode'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## ForceExecuteBindingNodeBySceneTagAndNodeTag

```ts
{
	commandId: 'ForceExecuteBindingNodeBySceneTagAndNodeTag'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: String
		$commandResult: Array
	}
}
``` 

## StartEventTrackPlayback

```ts
{
	commandId: 'StartEventTrackPlayback'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
		userToken: Array
		sessionId: Number
	}
}
``` 

## StopEventTrackPlayback

```ts
{
	commandId: 'StopEventTrackPlayback'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## StopAllEventTrackPlaybacks

```ts
{
	commandId: 'StopAllEventTrackPlaybacks'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## StopEventTrackPlaybackByQuery

```ts
{
	commandId: 'StopEventTrackPlaybackByQuery'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SetSyncValueRate

```ts
{
	commandId: 'SetSyncValueRate'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Number
	}
}
``` 

## SetSyncValueTickRate

```ts
{
	commandId: 'SetSyncValueTickRate'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Number
	}
}
``` 

## DestroySyncValue

```ts
{
	commandId: 'DestroySyncValue'
	{
		tx: String
		commandClientId: String
		createdAt: String
	}
}
``` 

## SendTargetAction

```ts
{
	commandId: 'SendTargetAction'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
		userToken: Object
		targetId: Object
	}
}
``` 

## ExecTargetAction

```ts
{
	commandId: 'ExecTargetAction'
	{
		tx: String
		commandClientId: String
		createdAt: withType
		lineage: Object
		$commandResult: Object
	}
}
``` 

## CreateCamera

```ts
{
	commandId: 'CreateCamera'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
	}
}
``` 

## DeleteCamera

```ts
{
	commandId: 'DeleteCamera'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## RenameCamera

```ts
{
	commandId: 'RenameCamera'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetCameraRouterInfo

```ts
{
	commandId: 'SetCameraRouterInfo'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetCameraResolution

```ts
{
	commandId: 'SetCameraResolution'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetStillRoot

```ts
{
	commandId: 'SetStillRoot'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetSelectedCameraStill

```ts
{
	commandId: 'SetSelectedCameraStill'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetCameraGeometry

```ts
{
	commandId: 'SetCameraGeometry'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
	}
}
``` 

## SetCameraPosition

```ts
{
	commandId: 'SetCameraPosition'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
		userToken: Number
		id: Number
		x: Number
		y: Number
	}
}
``` 

## CreateCalibration

```ts
{
	commandId: 'CreateCalibration'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## DeleteCalibration

```ts
{
	commandId: 'DeleteCalibration'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SetCalibrationType

```ts
{
	commandId: 'SetCalibrationType'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetStaticCalibration

```ts
{
	commandId: 'SetStaticCalibration'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreateSessionVariable

```ts
{
	commandId: 'CreateSessionVariable'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
	}
}
``` 

## DeleteSessionVariable

```ts
{
	commandId: 'DeleteSessionVariable'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SetSessionVariableType

```ts
{
	commandId: 'SetSessionVariableType'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## AssertSessionVariable

```ts
{
	commandId: 'AssertSessionVariable'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## DeleteSessionVariableValue

```ts
{
	commandId: 'DeleteSessionVariableValue'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SetSessionVariableValue

```ts
{
	commandId: 'SetSessionVariableValue'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## CreateBindingNodeConnection

```ts
{
	commandId: 'CreateBindingNodeConnection'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
		userToken: Object
		sceneId: Object
	}
}
``` 

## DeleteBindingNodeConnection

```ts
{
	commandId: 'DeleteBindingNodeConnection'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
		userToken: Object
	}
}
``` 

## DeleteBindingNodeConnections

```ts
{
	commandId: 'DeleteBindingNodeConnections'
	{
		tx: String
		commandClientId: String
		createdAt: Array
	}
}
``` 

## MoveBindingNode

```ts
{
	commandId: 'MoveBindingNode'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
		userToken: Number
		nodeId: Number
	}
}
``` 

## MoveBindingNodeStructured

```ts
{
	commandId: 'MoveBindingNodeStructured'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
	}
}
``` 

## SetBindingNodeValue

```ts
{
	commandId: 'SetBindingNodeValue'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## UpdateBindNodeValueAnchor

```ts
{
	commandId: 'UpdateBindNodeValueAnchor'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## ArmBundle

```ts
{
	commandId: 'ArmBundle'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## DisarmBundle

```ts
{
	commandId: 'DisarmBundle'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreatePoint

```ts
{
	commandId: 'CreatePoint'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
		userToken: Number
		projectId: String
		x: String
	}
}
``` 

## DeletePoint

```ts
{
	commandId: 'DeletePoint'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## RenamePoint

```ts
{
	commandId: 'RenamePoint'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetPointDescription

```ts
{
	commandId: 'SetPointDescription'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## MovePoint

```ts
{
	commandId: 'MovePoint'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
		userToken: Number
	}
}
``` 

## CreateCalibrationPoint

```ts
{
	commandId: 'CreateCalibrationPoint'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
		userToken: Object
	}
}
``` 

## DeleteCalibrationPoint

```ts
{
	commandId: 'DeleteCalibrationPoint'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SetCalibrationPointUV

```ts
{
	commandId: 'SetCalibrationPointUV'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
	}
}
``` 

## SelectPointForCalibrationPoint

```ts
{
	commandId: 'SelectPointForCalibrationPoint'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetCalibrationPointActive

```ts
{
	commandId: 'SetCalibrationPointActive'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Boolean
	}
}
``` 

## CreateClusterTargetAssign

```ts
{
	commandId: 'CreateClusterTargetAssign'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
		userToken: Object
	}
}
``` 

## RemoveClusterTargetAssign

```ts
{
	commandId: 'RemoveClusterTargetAssign'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## AssignAllTargets

```ts
{
	commandId: 'AssignAllTargets'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## RemoveAllTargets

```ts
{
	commandId: 'RemoveAllTargets'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreateColorProfile

```ts
{
	commandId: 'CreateColorProfile'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: String
		userToken: String
		projectId: Object
	}
}
``` 

## DeleteColorProfile

```ts
{
	commandId: 'DeleteColorProfile'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## RenameColorProfile

```ts
{
	commandId: 'RenameColorProfile'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetColorProfileWhiteBalance

```ts
{
	commandId: 'SetColorProfileWhiteBalance'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetColorProfileColorChecker

```ts
{
	commandId: 'SetColorProfileColorChecker'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetColorProfileExposure

```ts
{
	commandId: 'SetColorProfileExposure'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetColorProfileCamera

```ts
{
	commandId: 'SetColorProfileCamera'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreateConstraint

```ts
{
	commandId: 'CreateConstraint'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
		$commandResult: Object
		userToken: Object
		name: String
		data: String
		calendarId: Number
		scopeId: Boolean
	}
}
``` 

## SetConstraintType

```ts
{
	commandId: 'SetConstraintType'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetConstraintData

```ts
{
	commandId: 'SetConstraintData'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetConstraintContractInfo

```ts
{
	commandId: 'SetConstraintContractInfo'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: String
		userToken: Number
		id: Boolean
	}
}
``` 

## DeleteConstraint

```ts
{
	commandId: 'DeleteConstraint'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreatePlaylistFromAppearances

```ts
{
	commandId: 'CreatePlaylistFromAppearances'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Array
		$commandResult: Boolean
	}
}
``` 

## CreateSequence

```ts
{
	commandId: 'CreateSequence'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: String
		userToken: Object
	}
}
``` 

## RenameSequence

```ts
{
	commandId: 'RenameSequence'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## DeleteSequence

```ts
{
	commandId: 'DeleteSequence'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SetSequenceOptions

```ts
{
	commandId: 'SetSequenceOptions'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreateCue

```ts
{
	commandId: 'CreateCue'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## DeleteCue

```ts
{
	commandId: 'DeleteCue'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SetFollowConfig

```ts
{
	commandId: 'SetFollowConfig'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetDurationConfig

```ts
{
	commandId: 'SetDurationConfig'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetCueType

```ts
{
	commandId: 'SetCueType'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## AssertCueOrder

```ts
{
	commandId: 'AssertCueOrder'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## StartCue

```ts
{
	commandId: 'StartCue'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## StartCueBySequenceNameAndCueNumber

```ts
{
	commandId: 'StartCueBySequenceNameAndCueNumber'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Number
		$commandResult: Array
	}
}
``` 

## StopCue

```ts
{
	commandId: 'StopCue'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## TriggerFollowsForCue

```ts
{
	commandId: 'TriggerFollowsForCue'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetCueEnded

```ts
{
	commandId: 'SetCueEnded'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreateCurve

```ts
{
	commandId: 'CreateCurve'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateCurve

```ts
{
	commandId: 'UpdateCurve'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## DeleteCurve

```ts
{
	commandId: 'DeleteCurve'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## RenameCurve

```ts
{
	commandId: 'RenameCurve'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## DuplicateCurve

```ts
{
	commandId: 'DuplicateCurve'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreateDataTransfer

```ts
{
	commandId: 'CreateDataTransfer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: String
	}
}
``` 

## SaveDataTransfer

```ts
{
	commandId: 'SaveDataTransfer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## DeleteDataTransfer

```ts
{
	commandId: 'DeleteDataTransfer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## RegisterDevice

```ts
{
	commandId: 'RegisterDevice'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## GenerateClaimCode

```ts
{
	commandId: 'GenerateClaimCode'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
	}
}
``` 

## ClaimDevice

```ts
{
	commandId: 'ClaimDevice'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
		$commandResult: Object
	}
}
``` 

## ApproveDeviceClaim

```ts
{
	commandId: 'ApproveDeviceClaim'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## RejectDeviceClaim

```ts
{
	commandId: 'RejectDeviceClaim'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: String
	}
}
``` 

## TransferDeviceOwnership

```ts
{
	commandId: 'TransferDeviceOwnership'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## AcceptDeviceTransfer

```ts
{
	commandId: 'AcceptDeviceTransfer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CancelDeviceTransfer

```ts
{
	commandId: 'CancelDeviceTransfer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## ShareDevice

```ts
{
	commandId: 'ShareDevice'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
		userToken: Object
		machineId: Object
	}
}
``` 

## UpdateDeviceShare

```ts
{
	commandId: 'UpdateDeviceShare'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## RevokeDeviceShare

```ts
{
	commandId: 'RevokeDeviceShare'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UnclaimDevice

```ts
{
	commandId: 'UnclaimDevice'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreateEventTrackLayer

```ts
{
	commandId: 'CreateEventTrackLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## DeleteEventTrackLayer

```ts
{
	commandId: 'DeleteEventTrackLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## RenameEventTrackLayer

```ts
{
	commandId: 'RenameEventTrackLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## DeleteMachine

```ts
{
	commandId: 'DeleteMachine'
	{
		tx: String
		commandClientId: String
		createdAt: String
	}
}
``` 

## DeleteOfflineMachines

```ts
{
	commandId: 'DeleteOfflineMachines'
	{
		tx: String
		commandClientId: String
	}
}
``` 

## UpdateMachineVpnStatus

```ts
{
	commandId: 'UpdateMachineVpnStatus'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Array
	}
}
``` 

## UpdateMachineWanMac

```ts
{
	commandId: 'UpdateMachineWanMac'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: String
	}
}
``` 

## UpdateMachineDaemonVersion

```ts
{
	commandId: 'UpdateMachineDaemonVersion'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: String
	}
}
``` 

## UpdateMachineHostname

```ts
{
	commandId: 'UpdateMachineHostname'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: String
	}
}
``` 

## SetMachineJnAuth

```ts
{
	commandId: 'SetMachineJnAuth'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: String
		$commandResult: String
	}
}
``` 

## ClearMachineJnAuth

```ts
{
	commandId: 'ClearMachineJnAuth'
	{
		tx: String
		commandClientId: String
		createdAt: String
	}
}
``` 

## CompleteMachineMigration

```ts
{
	commandId: 'CompleteMachineMigration'
	{
		tx: String
		commandClientId: String
		createdAt: String
	}
}
``` 

## CreateMachine

```ts
{
	commandId: 'CreateMachine'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
	}
}
``` 

## CreateFeed

```ts
{
	commandId: 'CreateFeed'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
	}
}
``` 

## SetFeedRotation

```ts
{
	commandId: 'SetFeedRotation'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
	}
}
``` 

## SetFeedName

```ts
{
	commandId: 'SetFeedName'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetFeedInputPosition

```ts
{
	commandId: 'SetFeedInputPosition'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
	}
}
``` 

## SetFeedOutputPosition

```ts
{
	commandId: 'SetFeedOutputPosition'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
	}
}
``` 

## SetFeedSize

```ts
{
	commandId: 'SetFeedSize'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
	}
}
``` 

## DeleteFeed

```ts
{
	commandId: 'DeleteFeed'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## ClearFilesForMachine

```ts
{
	commandId: 'ClearFilesForMachine'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## DeleteFileTransfer

```ts
{
	commandId: 'DeleteFileTransfer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreateDMXUniverse

```ts
{
	commandId: 'CreateDMXUniverse'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
		$commandResult: Number
		userToken: Object
	}
}
``` 

## DeleteDMXUniverse

```ts
{
	commandId: 'DeleteDMXUniverse'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateDMXUniverse

```ts
{
	commandId: 'UpdateDMXUniverse'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetDMXUniverseOutput

```ts
{
	commandId: 'SetDMXUniverseOutput'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Boolean
	}
}
``` 

## CreateFixture

```ts
{
	commandId: 'CreateFixture'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
		$commandResult: Object
		userToken: Number
		name: Number
		projectId: Number
		fixtureTypeId: Number
		x: Number
		y: Number
		z: Number
		rotX: Number
		rotY: String
		rotZ: Object
		universe: String
		address: Object
	}
}
``` 

## DeleteFixture

```ts
{
	commandId: 'DeleteFixture'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateFixture

```ts
{
	commandId: 'UpdateFixture'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetFixtureGeometry

```ts
{
	commandId: 'SetFixtureGeometry'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
	}
}
``` 

## CreateFixtureType

```ts
{
	commandId: 'CreateFixtureType'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: String
		$commandResult: Number
		userToken: Number
		name: Object
		manufacturer: Array
		beamAngle: Boolean
		fieldAngle: Boolean
		colorMixing: Boolean
		dmxModes: String
		hasGobo: String
		hasPanTilt: Number
		hasZoom: Number
		gdtfUrl: String
		geometryUrl: Number
		weight: Number
		power: Number
		iesProfileUrl: Number
		lumens: Number
		colorTemperature: Array
		cri: Array
		maxPan: Array
	}
}
``` 

## DeleteFixtureType

```ts
{
	commandId: 'DeleteFixtureType'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateFixtureType

```ts
{
	commandId: 'UpdateFixtureType'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreateFixtureCalibration

```ts
{
	commandId: 'CreateFixtureCalibration'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: String
		userToken: Object
	}
}
``` 

## DeleteFixtureCalibration

```ts
{
	commandId: 'DeleteFixtureCalibration'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## RenameFixtureCalibration

```ts
{
	commandId: 'RenameFixtureCalibration'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetFixtureCalibrationDimmerCurve

```ts
{
	commandId: 'SetFixtureCalibrationDimmerCurve'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
		$commandResult: Number
	}
}
``` 

## SetFixtureCalibrationDimmerCurvePreset

```ts
{
	commandId: 'SetFixtureCalibrationDimmerCurvePreset'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetFixtureCalibrationColorCalibrations

```ts
{
	commandId: 'SetFixtureCalibrationColorCalibrations'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
		$commandResult: Number
	}
}
``` 

## SetFixtureCalibrationBeamProfile

```ts
{
	commandId: 'SetFixtureCalibrationBeamProfile'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
		userToken: Number
	}
}
``` 

## SetFixtureCalibrationReference

```ts
{
	commandId: 'SetFixtureCalibrationReference'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: String
	}
}
``` 

## CreateFixtureGroup

```ts
{
	commandId: 'CreateFixtureGroup'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
		$commandResult: Object
	}
}
``` 

## DeleteFixtureGroup

```ts
{
	commandId: 'DeleteFixtureGroup'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateFixtureGroup

```ts
{
	commandId: 'UpdateFixtureGroup'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## AddFixturesToGroup

```ts
{
	commandId: 'AddFixturesToGroup'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## RemoveFixturesFromGroup

```ts
{
	commandId: 'RemoveFixturesFromGroup'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## SetFixtureGroupMaster

```ts
{
	commandId: 'SetFixtureGroupMaster'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
	}
}
``` 

## CreateFixturePreset

```ts
{
	commandId: 'CreateFixturePreset'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
		$commandResult: Array
		userToken: Object
	}
}
``` 

## DeleteFixturePreset

```ts
{
	commandId: 'DeleteFixturePreset'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateFixturePreset

```ts
{
	commandId: 'UpdateFixturePreset'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetFixturePresetValues

```ts
{
	commandId: 'SetFixturePresetValues'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## UpdateFixturePresetValue

```ts
{
	commandId: 'UpdateFixturePresetValue'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## RemoveFixtureFromPreset

```ts
{
	commandId: 'RemoveFixtureFromPreset'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreateInstanceAssign

```ts
{
	commandId: 'CreateInstanceAssign'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## DeleteInstanceAssign

```ts
{
	commandId: 'DeleteInstanceAssign'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## ClearInstanceAssignsByInstance

```ts
{
	commandId: 'ClearInstanceAssignsByInstance'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## ClearInstanceAssignsBySessionAndService

```ts
{
	commandId: 'ClearInstanceAssignsBySessionAndService'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreateInvite

```ts
{
	commandId: 'CreateInvite'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: String
		userToken: Object
		inviteType: Object
		entityId: Object
	}
}
``` 

## AcceptInvite

```ts
{
	commandId: 'AcceptInvite'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
	}
}
``` 

## DeclineInvite

```ts
{
	commandId: 'DeclineInvite'
	{
		tx: String
		commandClientId: String
		createdAt: String
	}
}
``` 

## RevokeInvite

```ts
{
	commandId: 'RevokeInvite'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## ResendInvite

```ts
{
	commandId: 'ResendInvite'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreateInstanceClusterAssign

```ts
{
	commandId: 'CreateInstanceClusterAssign'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## DeleteInstanceClusterAssign

```ts
{
	commandId: 'DeleteInstanceClusterAssign'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## ClearInstanceClusterAssignsByCluster

```ts
{
	commandId: 'ClearInstanceClusterAssignsByCluster'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreateKeyframe

```ts
{
	commandId: 'CreateKeyframe'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Number
		userToken: Object
	}
}
``` 

## MoveKeyframe

```ts
{
	commandId: 'MoveKeyframe'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
	}
}
``` 

## SaveKeyframe

```ts
{
	commandId: 'SaveKeyframe'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## DeleteKeyframe

```ts
{
	commandId: 'DeleteKeyframe'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SetKeyframeCurve

```ts
{
	commandId: 'SetKeyframeCurve'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreateMapping

```ts
{
	commandId: 'CreateMapping'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
		userToken: Object
		projectId: Array
		name: Object
	}
}
``` 

## DeleteMapping

```ts
{
	commandId: 'DeleteMapping'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateMapping

```ts
{
	commandId: 'UpdateMapping'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## UpdateFeedRectangle

```ts
{
	commandId: 'UpdateFeedRectangle'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Object
	}
}
``` 

## AddFeedRectangle

```ts
{
	commandId: 'AddFeedRectangle'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## RemoveFeedRectangle

```ts
{
	commandId: 'RemoveFeedRectangle'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
	}
}
``` 

## LinkMappingToDisplay

```ts
{
	commandId: 'LinkMappingToDisplay'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: String
		userToken: String
		id: String
	}
}
``` 

## UnlinkMapping

```ts
{
	commandId: 'UnlinkMapping'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## ReportMappingSyncStatus

```ts
{
	commandId: 'ReportMappingSyncStatus'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: String
	}
}
``` 

## CreateLayer

```ts
{
	commandId: 'CreateLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: String
		userToken: Object
	}
}
``` 

## DeleteLayer

```ts
{
	commandId: 'DeleteLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateLayer

```ts
{
	commandId: 'UpdateLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## MoveLayer

```ts
{
	commandId: 'MoveLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
	}
}
``` 

## SetLayerOpacity

```ts
{
	commandId: 'SetLayerOpacity'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
	}
}
``` 

## SetLayerEnabled

```ts
{
	commandId: 'SetLayerEnabled'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Boolean
	}
}
``` 

## SetLayerBlendMode

```ts
{
	commandId: 'SetLayerBlendMode'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetLayerSolo

```ts
{
	commandId: 'SetLayerSolo'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Boolean
	}
}
``` 

## CreateLayerGroup

```ts
{
	commandId: 'CreateLayerGroup'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
	}
}
``` 

## DeleteLayerGroup

```ts
{
	commandId: 'DeleteLayerGroup'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateLayerGroup

```ts
{
	commandId: 'UpdateLayerGroup'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## AddLayerToGroup

```ts
{
	commandId: 'AddLayerToGroup'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Number
	}
}
``` 

## RemoveLayerFromGroup

```ts
{
	commandId: 'RemoveLayerFromGroup'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## ReorderLayersInGroup

```ts
{
	commandId: 'ReorderLayersInGroup'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## SetLayerGroupOrder

```ts
{
	commandId: 'SetLayerGroupOrder'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
	}
}
``` 

## SetLayerGroupOpacity

```ts
{
	commandId: 'SetLayerGroupOpacity'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
	}
}
``` 

## SetLayerGroupEnabled

```ts
{
	commandId: 'SetLayerGroupEnabled'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Boolean
	}
}
``` 

## SetLayerGroupCollapsed

```ts
{
	commandId: 'SetLayerGroupCollapsed'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Boolean
	}
}
``` 

## CreateLEDWall

```ts
{
	commandId: 'CreateLEDWall'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Number
		userToken: Number
		projectId: Number
		name: Number
		x: Number
		y: Number
		z: Number
		width: Number
		height: Object
	}
}
``` 

## DeleteLEDWall

```ts
{
	commandId: 'DeleteLEDWall'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateLEDWall

```ts
{
	commandId: 'UpdateLEDWall'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## MoveLEDWall

```ts
{
	commandId: 'MoveLEDWall'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
		userToken: Number
	}
}
``` 

## RotateLEDWall

```ts
{
	commandId: 'RotateLEDWall'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
		userToken: Number
	}
}
``` 

## SetLEDWallContent

```ts
{
	commandId: 'SetLEDWallContent'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetLEDWallBrightness

```ts
{
	commandId: 'SetLEDWallBrightness'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
	}
}
``` 

## SetLEDWallGeometry

```ts
{
	commandId: 'SetLEDWallGeometry'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
	}
}
``` 

## CreateLensFile

```ts
{
	commandId: 'CreateLensFile'
	{
		tx: String
		commandClientId: String
		createdAt: String
	}
}
``` 

## DeleteLensFile

```ts
{
	commandId: 'DeleteLensFile'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## RenameLensFile

```ts
{
	commandId: 'RenameLensFile'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## DeleteLinkExec

```ts
{
	commandId: 'DeleteLinkExec'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## StartExec

```ts
{
	commandId: 'StartExec'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## DeleteLinkExecRunner

```ts
{
	commandId: 'DeleteLinkExecRunner'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## StopExecRunner

```ts
{
	commandId: 'StopExecRunner'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreateLocation

```ts
{
	commandId: 'CreateLocation'
	{
		tx: String
		commandClientId: String
	}
}
``` 

## CreateMeasurement

```ts
{
	commandId: 'CreateMeasurement'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
		userToken: Array
		projectId: Number
		name: String
		type: String
		points: String
	}
}
``` 

## DeleteMeasurement

```ts
{
	commandId: 'DeleteMeasurement'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateMeasurement

```ts
{
	commandId: 'UpdateMeasurement'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreateNetwork

```ts
{
	commandId: 'CreateNetwork'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
		$commandResult: String
		userToken: Object
	}
}
``` 

## UpdateNetwork

```ts
{
	commandId: 'UpdateNetwork'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## DeleteNetwork

```ts
{
	commandId: 'DeleteNetwork'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## JoinNetwork

```ts
{
	commandId: 'JoinNetwork'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## LeaveNetwork

```ts
{
	commandId: 'LeaveNetwork'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## ApproveNetworkMember

```ts
{
	commandId: 'ApproveNetworkMember'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateNetworkMemberStatus

```ts
{
	commandId: 'UpdateNetworkMemberStatus'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Array
		userToken: String
	}
}
``` 

## UpdateNetworkMemberLastSeen

```ts
{
	commandId: 'UpdateNetworkMemberLastSeen'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreateOrganization

```ts
{
	commandId: 'CreateOrganization'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: String
		$commandResult: Object
	}
}
``` 

## UpdateOrganization

```ts
{
	commandId: 'UpdateOrganization'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## DeleteOrganization

```ts
{
	commandId: 'DeleteOrganization'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateOrganizationSubscription

```ts
{
	commandId: 'UpdateOrganizationSubscription'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## AssertOverviewNodes

```ts
{
	commandId: 'AssertOverviewNodes'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## MoveOverviewNode

```ts
{
	commandId: 'MoveOverviewNode'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## QuickLinkScenes

```ts
{
	commandId: 'QuickLinkScenes'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## AssertUser

```ts
{
	commandId: 'AssertUser'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreateUser

```ts
{
	commandId: 'CreateUser'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: String
		$commandResult: Object
	}
}
``` 

## UpdateUserProfile

```ts
{
	commandId: 'UpdateUserProfile'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetUserPrimaryOrganization

```ts
{
	commandId: 'SetUserPrimaryOrganization'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## UpdateUserStatus

```ts
{
	commandId: 'UpdateUserStatus'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## RecordUserLogin

```ts
{
	commandId: 'RecordUserLogin'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## GenerateUserApiKey

```ts
{
	commandId: 'GenerateUserApiKey'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## RevokeUserApiKey

```ts
{
	commandId: 'RevokeUserApiKey'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## DeleteUser

```ts
{
	commandId: 'DeleteUser'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreatePane

```ts
{
	commandId: 'CreatePane'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
		userToken: Object
		ownerId: Object
		windowGroupId: Object
	}
}
``` 

## SetPaneActiveView

```ts
{
	commandId: 'SetPaneActiveView'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetPaneViewState

```ts
{
	commandId: 'SetPaneViewState'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetPaneSession

```ts
{
	commandId: 'SetPaneSession'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetPaneProject

```ts
{
	commandId: 'SetPaneProject'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetPaneSelectedTargets

```ts
{
	commandId: 'SetPaneSelectedTargets'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## SetPaneSelectedBundles

```ts
{
	commandId: 'SetPaneSelectedBundles'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## PopOutPane

```ts
{
	commandId: 'PopOutPane'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## DockPane

```ts
{
	commandId: 'DockPane'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## DeletePane

```ts
{
	commandId: 'DeletePane'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreatePort

```ts
{
	commandId: 'CreatePort'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: String
		userToken: Object
	}
}
``` 

## UpdatePort

```ts
{
	commandId: 'UpdatePort'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## DeletePort

```ts
{
	commandId: 'DeletePort'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## ConfigurePortBridge

```ts
{
	commandId: 'ConfigurePortBridge'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## DisablePortBridge

```ts
{
	commandId: 'DisablePortBridge'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdatePortBridgeStatus

```ts
{
	commandId: 'UpdatePortBridgeStatus'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: String
	}
}
``` 

## ArmPayloads

```ts
{
	commandId: 'ArmPayloads'
	{
		tx: String
		commandClientId: String
		createdAt: Array
		lineage: Object
	}
}
``` 

## DisarmPayloads

```ts
{
	commandId: 'DisarmPayloads'
	{
		tx: String
		commandClientId: String
		createdAt: Array
		lineage: Object
	}
}
``` 

## CreatePlaylist

```ts
{
	commandId: 'CreatePlaylist'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
	}
}
``` 

## CalculatePlaylistDuration

```ts
{
	commandId: 'CalculatePlaylistDuration'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SetPlaylistName

```ts
{
	commandId: 'SetPlaylistName'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetPlaylistDescription

```ts
{
	commandId: 'SetPlaylistDescription'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetPlaylistTags

```ts
{
	commandId: 'SetPlaylistTags'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## DeletePlaylist

```ts
{
	commandId: 'DeletePlaylist'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## TrimPlaylist

```ts
{
	commandId: 'TrimPlaylist'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreatePlaylistAppearance

```ts
{
	commandId: 'CreatePlaylistAppearance'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: String
	}
}
``` 

## DeletePlaylistAppearance

```ts
{
	commandId: 'DeletePlaylistAppearance'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SetPlaylistAppearanceStartTime

```ts
{
	commandId: 'SetPlaylistAppearanceStartTime'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## CreatePlaylistItem

```ts
{
	commandId: 'CreatePlaylistItem'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: String
		userToken: String
	}
}
``` 

## SetPlaylistItemTimes

```ts
{
	commandId: 'SetPlaylistItemTimes'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: String
	}
}
``` 

## DeletePlaylistItem

```ts
{
	commandId: 'DeletePlaylistItem'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreateProp

```ts
{
	commandId: 'CreateProp'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Number
		userToken: Number
		projectId: Number
		name: Number
		x: Number
		y: Number
		z: String
		rotX: String
		rotY: Object
		rotZ: String
	}
}
``` 

## DeleteProp

```ts
{
	commandId: 'DeleteProp'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## RenameProp

```ts
{
	commandId: 'RenameProp'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetPropPosition

```ts
{
	commandId: 'SetPropPosition'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
		userToken: Number
		id: Number
		x: Number
		y: Number
	}
}
``` 

## SetPropScale

```ts
{
	commandId: 'SetPropScale'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
		userToken: Number
	}
}
``` 

## SetPropGeometry

```ts
{
	commandId: 'SetPropGeometry'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
	}
}
``` 

## SetPropVisibility

```ts
{
	commandId: 'SetPropVisibility'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Boolean
	}
}
``` 

## SetPropColor

```ts
{
	commandId: 'SetPropColor'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## CreateResolutionStrategy

```ts
{
	commandId: 'CreateResolutionStrategy'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
	}
}
``` 

## UpdateResolutionStrategy

```ts
{
	commandId: 'UpdateResolutionStrategy'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
	}
}
``` 

## GrantRole

```ts
{
	commandId: 'GrantRole'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
		userToken: Object
		userId: Object
	}
}
``` 

## UpdateRole

```ts
{
	commandId: 'UpdateRole'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## RevokeRole

```ts
{
	commandId: 'RevokeRole'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## RevokeAllRolesForEntity

```ts
{
	commandId: 'RevokeAllRolesForEntity'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## TransferOwnership

```ts
{
	commandId: 'TransferOwnership'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
		userToken: Object
	}
}
``` 

## CreateScreen

```ts
{
	commandId: 'CreateScreen'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
		userToken: Number
		projectId: Number
		name: Number
		type: Number
		x: Number
		y: Number
		z: Number
		width: Object
	}
}
``` 

## DeleteScreen

```ts
{
	commandId: 'DeleteScreen'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateScreen

```ts
{
	commandId: 'UpdateScreen'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## MoveScreen

```ts
{
	commandId: 'MoveScreen'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
		userToken: Number
	}
}
``` 

## RotateScreen

```ts
{
	commandId: 'RotateScreen'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
		userToken: Number
	}
}
``` 

## SetScreenColorCorrection

```ts
{
	commandId: 'SetScreenColorCorrection'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Boolean
		$commandResult: Number
		userToken: Number
		id: Number
	}
}
``` 

## SetScreenTestPattern

```ts
{
	commandId: 'SetScreenTestPattern'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Boolean
		$commandResult: String
	}
}
``` 

## SetScreenContent

```ts
{
	commandId: 'SetScreenContent'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetScreenBrightness

```ts
{
	commandId: 'SetScreenBrightness'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
	}
}
``` 

## SetScreenGeometry

```ts
{
	commandId: 'SetScreenGeometry'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
	}
}
``` 

## LinkScreenToDisplay

```ts
{
	commandId: 'LinkScreenToDisplay'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: String
		userToken: String
		id: String
		physicalDisplayId: Boolean
	}
}
``` 

## UnlinkScreen

```ts
{
	commandId: 'UnlinkScreen'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## RequestScreenSync

```ts
{
	commandId: 'RequestScreenSync'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## ReportScreenSyncResult

```ts
{
	commandId: 'ReportScreenSyncResult'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Boolean
		$commandResult: String
	}
}
``` 

## GenerateScreensFromDisplays

```ts
{
	commandId: 'GenerateScreensFromDisplays'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
		$commandResult: Boolean
	}
}
``` 

## CreateSequencePlayback

```ts
{
	commandId: 'CreateSequencePlayback'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## DeleteSequencePlayback

```ts
{
	commandId: 'DeleteSequencePlayback'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SetSequencePlaybackLoopMode

```ts
{
	commandId: 'SetSequencePlaybackLoopMode'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetSequencePlaybackLoopModeByBySession

```ts
{
	commandId: 'SetSequencePlaybackLoopModeByBySession'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
	}
}
``` 

## MoveToNextCue

```ts
{
	commandId: 'MoveToNextCue'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## MoveToPreviousCue

```ts
{
	commandId: 'MoveToPreviousCue'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## MoveToNextCueByName

```ts
{
	commandId: 'MoveToNextCueByName'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Array
	}
}
``` 

## MoveToPreviousCueByName

```ts
{
	commandId: 'MoveToPreviousCueByName'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Array
	}
}
``` 

## CreateSpace

```ts
{
	commandId: 'CreateSpace'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
	}
}
``` 

## DeleteSpace

```ts
{
	commandId: 'DeleteSpace'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## RenameSpace

```ts
{
	commandId: 'RenameSpace'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetSpaceServices

```ts
{
	commandId: 'SetSpaceServices'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## DeleteStream

```ts
{
	commandId: 'DeleteStream'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreateSync

```ts
{
	commandId: 'CreateSync'
	{
		tx: String
		commandClientId: String
	}
}
``` 

## AssertTagByName

```ts
{
	commandId: 'AssertTagByName'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
	}
}
``` 

## CreateTag

```ts
{
	commandId: 'CreateTag'
	{
		tx: String
		commandClientId: String
		createdAt: String
		lineage: Object
	}
}
``` 

## RenameTag

```ts
{
	commandId: 'RenameTag'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## DeleteTag

```ts
{
	commandId: 'DeleteTag'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## AssignTag

```ts
{
	commandId: 'AssignTag'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: String
	}
}
``` 

## RemoveTag

```ts
{
	commandId: 'RemoveTag'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: String
	}
}
``` 

## SetTargetStatusesOfflineByClient

```ts
{
	commandId: 'SetTargetStatusesOfflineByClient'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## CreateVideoLayer

```ts
{
	commandId: 'CreateVideoLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
	}
}
``` 

## DeleteVideoLayer

```ts
{
	commandId: 'DeleteVideoLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateVideoLayer

```ts
{
	commandId: 'UpdateVideoLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetVideoLayerStream

```ts
{
	commandId: 'SetVideoLayerStream'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetVideoLayerPlayback

```ts
{
	commandId: 'SetVideoLayerPlayback'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Object
	}
}
``` 

## SetVideoLayerColorAdjustments

```ts
{
	commandId: 'SetVideoLayerColorAdjustments'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
		userToken: Number
		id: Number
	}
}
``` 

## CreateVFXGraph

```ts
{
	commandId: 'CreateVFXGraph'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
	}
}
``` 

## DeleteVFXGraph

```ts
{
	commandId: 'DeleteVFXGraph'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateVFXGraph

```ts
{
	commandId: 'UpdateVFXGraph'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetVFXGraphData

```ts
{
	commandId: 'SetVFXGraphData'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetVFXGraphThumbnail

```ts
{
	commandId: 'SetVFXGraphThumbnail'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetVFXGraphCompilationError

```ts
{
	commandId: 'SetVFXGraphCompilationError'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Number
	}
}
``` 

## CloneVFXGraph

```ts
{
	commandId: 'CloneVFXGraph'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
	}
}
``` 

## SetVFXNodePosition

```ts
{
	commandId: 'SetVFXNodePosition'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Number
		userToken: Number
		graphId: Number
		nodeId: Number
	}
}
``` 

## SetVFXNodeCollapsed

```ts
{
	commandId: 'SetVFXNodeCollapsed'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Boolean
	}
}
``` 

## DeleteVFXNodePosition

```ts
{
	commandId: 'DeleteVFXNodePosition'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetVFXNodeValue

```ts
{
	commandId: 'SetVFXNodeValue'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: String
		userToken: Object
	}
}
``` 

## DeleteVFXNodeValue

```ts
{
	commandId: 'DeleteVFXNodeValue'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: String
	}
}
``` 

## CreateVFXConnection

```ts
{
	commandId: 'CreateVFXConnection'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: String
		userToken: String
		graphId: String
	}
}
``` 

## DeleteVFXConnection

```ts
{
	commandId: 'DeleteVFXConnection'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## DeleteVFXConnectionsByNodeId

```ts
{
	commandId: 'DeleteVFXConnectionsByNodeId'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## AddVFXNode

```ts
{
	commandId: 'AddVFXNode'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
		userToken: Object
	}
}
``` 

## DeleteVFXNode

```ts
{
	commandId: 'DeleteVFXNode'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## DeleteVFXNodes

```ts
{
	commandId: 'DeleteVFXNodes'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## CloneVFXNodes

```ts
{
	commandId: 'CloneVFXNodes'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
		$commandResult: Object
	}
}
``` 

## CreateVFXGraphLayer

```ts
{
	commandId: 'CreateVFXGraphLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: String
		userToken: Object
	}
}
``` 

## DeleteVFXGraphLayer

```ts
{
	commandId: 'DeleteVFXGraphLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateVFXGraphLayer

```ts
{
	commandId: 'UpdateVFXGraphLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetVFXGraphLayerInputBindings

```ts
{
	commandId: 'SetVFXGraphLayerInputBindings'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## SetVFXGraphLayerVFXGraph

```ts
{
	commandId: 'SetVFXGraphLayerVFXGraph'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreateRenderCluster

```ts
{
	commandId: 'CreateRenderCluster'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: Object
	}
}
``` 

## DeleteRenderCluster

```ts
{
	commandId: 'DeleteRenderCluster'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateRenderCluster

```ts
{
	commandId: 'UpdateRenderCluster'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetRenderClusterCoordinator

```ts
{
	commandId: 'SetRenderClusterCoordinator'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetRenderClusterStatus

```ts
{
	commandId: 'SetRenderClusterStatus'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetRenderClusterPaused

```ts
{
	commandId: 'SetRenderClusterPaused'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Boolean
	}
}
``` 

## UpdateRenderClusterFrame

```ts
{
	commandId: 'UpdateRenderClusterFrame'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Number
	}
}
``` 

## UpdateRenderClusterLatency

```ts
{
	commandId: 'UpdateRenderClusterLatency'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
	}
}
``` 

## JoinRenderCluster

```ts
{
	commandId: 'JoinRenderCluster'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: String
		userToken: String
		clusterId: Object
	}
}
``` 

## LeaveRenderCluster

```ts
{
	commandId: 'LeaveRenderCluster'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateRenderNodeStatus

```ts
{
	commandId: 'UpdateRenderNodeStatus'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: String
	}
}
``` 

## UpdateRenderNodeHeartbeat

```ts
{
	commandId: 'UpdateRenderNodeHeartbeat'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
		$commandResult: Object
	}
}
``` 

## AssignRenderNodeRegions

```ts
{
	commandId: 'AssignRenderNodeRegions'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## AssignRenderNodeScreens

```ts
{
	commandId: 'AssignRenderNodeScreens'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## SetRenderNodeStreamEndpoint

```ts
{
	commandId: 'SetRenderNodeStreamEndpoint'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetRenderNodeWebRTCPeerId

```ts
{
	commandId: 'SetRenderNodeWebRTCPeerId'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetRenderNodeClockOffset

```ts
{
	commandId: 'SetRenderNodeClockOffset'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Number
	}
}
``` 

## SetRenderNodeCoordinatorCandidate

```ts
{
	commandId: 'SetRenderNodeCoordinatorCandidate'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Boolean
	}
}
``` 

## CreateRiveProject

```ts
{
	commandId: 'CreateRiveProject'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
		$commandResult: String
		userToken: String
		projectId: Number
		name: String
		assetId: Object
	}
}
``` 

## DeleteRiveProject

```ts
{
	commandId: 'DeleteRiveProject'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateRiveProject

```ts
{
	commandId: 'UpdateRiveProject'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetRiveProjectThumbnail

```ts
{
	commandId: 'SetRiveProjectThumbnail'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetRiveProjectMetadata

```ts
{
	commandId: 'SetRiveProjectMetadata'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## CreateRiveLayer

```ts
{
	commandId: 'CreateRiveLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: String
		userToken: Object
	}
}
``` 

## DeleteRiveLayer

```ts
{
	commandId: 'DeleteRiveLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## UpdateRiveLayer

```ts
{
	commandId: 'UpdateRiveLayer'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetRiveLayerStateMachine

```ts
{
	commandId: 'SetRiveLayerStateMachine'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## SetRiveLayerAnimation

```ts
{
	commandId: 'SetRiveLayerAnimation'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: String
	}
}
``` 

## CreateWindowGroup

```ts
{
	commandId: 'CreateWindowGroup'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetWindowGroupSession

```ts
{
	commandId: 'SetWindowGroupSession'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetSelectedTargets

```ts
{
	commandId: 'SetSelectedTargets'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## SetProject

```ts
{
	commandId: 'SetProject'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SetSelectedBundles

```ts
{
	commandId: 'SetSelectedBundles'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## SetSelectedScreens

```ts
{
	commandId: 'SetSelectedScreens'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Array
	}
}
``` 

## DeleteWindowGroup

```ts
{
	commandId: 'DeleteWindowGroup'
	{
		tx: String
		commandClientId: String
		createdAt: Object
	}
}
``` 

## SetWindowGroupPaneLayout

```ts
{
	commandId: 'SetWindowGroupPaneLayout'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## SplitPane

```ts
{
	commandId: 'SplitPane'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Object
		userToken: Object
	}
}
``` 

## ClosePane

```ts
{
	commandId: 'ClosePane'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
	}
}
``` 

## ResizePaneSplit

```ts
{
	commandId: 'ResizePaneSplit'
	{
		tx: String
		commandClientId: String
		createdAt: Object
		lineage: Object
		$commandResult: Number
	}
}
``` 