module.exports = {
	name: 'Lina',
	init: function (options, callback) {
		console.log( this.name + ' inited...' + options)
		callback()
	},
	marieChanged: function ( payload, callback ) {
		this.hasMarieChanged = true
		console.log( '-----------', this.hasMarieChanged )
		callback( null, 'OK' )
	}
}
